package kawa_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/runreveal/kawa"
	"github.com/runreveal/kawa/x/memory"
	"github.com/runreveal/kawa/x/mqtt"
	"github.com/runreveal/kawa/x/printer"
	"github.com/runreveal/kawa/x/redis"
	"github.com/runreveal/kawa/x/scanner"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMem(t *testing.T) {
	schan := make(chan []byte)
	src := memory.NewMemSource((<-chan []byte)(schan))
	dst := memory.NewMemDestination[[]byte]((chan<- []byte)(schan))
	SuiteTest(t, src, dst)
}

func BenchmarkMem(b *testing.B) {
	schan := make(chan []byte)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := memory.NewMemSource((<-chan []byte)(schan))
		dst := memory.NewMemDestination[[]byte]((chan<- []byte)(schan))
		runner := BuildBench(b, 1000000, src, dst)
		b.StartTimer()
		err := runner.Run(context.Background())
		assert.NoError(b, err)
	}
}

func TestRedis(t *testing.T) {
	rc := goredis.NewClient(&goredis.Options{
		Addr:     "localhost:6379", // Redis server address
		Password: "",               // No password set
		DB:       0,                // Use default DB
	})
	_, err := rc.XGroupCreateMkStream(context.Background(), "kawa/topic", "kawa", "$").Result()
	if err != nil {
		fmt.Println("err initing", err)
	}

	time.Sleep(1 * time.Second)

	src := redis.NewSource(redis.WithAddr("localhost:6379"), redis.WithTopic("kawa/topic"))
	dst := redis.NewDestination(redis.WithAddr("localhost:6379"), redis.WithTopic("kawa/topic"))
	SuiteTest(t, src, dst)
}

func TestIO(t *testing.T) {
	reader, writer := io.Pipe()
	// Delim should be a string of bytes which is unlikely occur randomly
	scansrc := scanner.NewScanner(reader, scanner.WithDelim([]byte("0x0x0x0x0")))
	printdst := printer.NewPrinter(writer, printer.WithDelim([]byte("0x0x0x0x0")))
	time.AfterFunc(100*time.Millisecond, func() {
		// close the writer to signal the end of the stream
		// there's no easy way to do this implicitly without making
		// readers/writers into closers.  Maybe that's worth it?
		writer.Close()
	})
	SuiteTest(t, scansrc, printdst)
}

func TestMQTT(t *testing.T) {
	mqttOpts := []mqtt.OptFunc{
		mqtt.WithBroker("mqtt://localhost:1883"),
		mqtt.WithTopic("kawa/topic"),
		mqtt.WithKeepAlive(5 * time.Second),
		mqtt.WithQOS(2),
	}

	// mqttSrc.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
	// mqttSrc.CRITICAL = log.New(os.Stdout, "[CRIT] ", 0)
	// mqttSrc.WARN = log.New(os.Stdout, "[WARN]  ", 0)
	// mqttSrc.DEBUG = log.New(os.Stdout, "[DEBUG] ", 0)

	mqttsrc, err := mqtt.NewSource(append(mqttOpts, mqtt.WithClientID(ksuid.New().String()))...)
	require.NoError(t, err, "source create should not error")
	mqttdst, err := mqtt.NewDestination(append(mqttOpts, mqtt.WithClientID(ksuid.New().String()))...)
	require.NoError(t, err, "dest create should not error")
	SuiteTest(t, mqttsrc, mqttdst)
}

// Old tests

type BinString string

func (b *BinString) UnmarshalBinary(in []byte) error {
	*b = BinString(in)
	return nil
}

func (b BinString) MarshalBinary() ([]byte, error) {
	return []byte(b), nil
}

func TestSmokeHappyPath(t *testing.T) {
	schan := make(chan *BinString)
	src := memory.NewMemSource((<-chan *BinString)(schan))
	dst := memory.NewMemDestination[*BinString]((chan<- *BinString)(schan))
	var wg sync.WaitGroup
	wg.Add(10)

	go func() {
		for i := 0; i < 10; i++ {
			x := BinString(fmt.Sprintf("hi-%d", i))
			err := dst.Send(context.TODO(), nil, kawa.Message[*BinString]{Value: &x})
			if err != nil {
				t.Log(err)
			}
		}
	}()
	go func(t *testing.T) {
		t.Helper()
		for i := 0; i < 10; i++ {
			msg, _, err := src.Recv(context.TODO())
			if err != nil {
				t.Errorf("%v", err)
			}
			fmt.Printf("%s\n", *msg.Value)
			wg.Done()
		}
	}(t)
	wg.Wait()
}

// TestWow shows an example of creating a source without having to consume a
// generic implementation of a source this of course assumes the source is
// aware of the type that you're looking to surface to the pipeline
func TestWow(t *testing.T) {
	schan := make(chan *BinString)
	src := NewMemSource((<-chan *BinString)(schan))
	dst := memory.NewMemDestination[*BinString]((chan<- *BinString)(schan))
	var wg sync.WaitGroup
	wg.Add(10)

	go func() {
		for i := 0; i < 10; i++ {
			x := BinString(fmt.Sprintf("wow-%d", i))
			err := dst.Send(context.TODO(), nil, kawa.Message[*BinString]{Value: &x})
			if err != nil {
				t.Log(err)
			}
		}
	}()
	go func(t *testing.T) {
		t.Helper()
		for i := 0; i < 10; i++ {
			msg, _, err := src.Recv(context.TODO())
			if err != nil {
				t.Errorf("%v", err)
			}
			fmt.Printf("%s\n", *msg.Value)
			wg.Done()
		}
	}(t)
	wg.Wait()
}

type MemorySource struct {
	MsgC <-chan *BinString
}

func NewMemSource(in <-chan *BinString) MemorySource {
	return MemorySource{
		MsgC: in,
	}
}

func (ms MemorySource) Recv(ctx context.Context) (kawa.Message[*BinString], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[*BinString]{}, nil, ctx.Err()
	case v := <-ms.MsgC:
		return kawa.Message[*BinString]{Value: v}, nil, nil
	}
}
