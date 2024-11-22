package redis

import (
	"context"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
	"github.com/runreveal/kawa"
	"github.com/segmentio/ksuid"
)

type Options struct {
	topic string
	opts  goredis.Options
}

type Destination struct {
	opts   Options
	client *goredis.Client
}

type Option func(*Options)

func WithAddr(addr string) Option {
	return func(d *Options) {
		d.opts.Addr = addr
	}
}

func WithPassword(password string) Option {
	return func(d *Options) {
		d.opts.Password = password
	}
}

func WithUsername(username string) Option {
	return func(d *Options) {
		d.opts.Username = username
	}
}

func WithTopic(topic string) Option {
	return func(d *Options) {
		d.topic = topic
	}
}

func NewDestination(opts ...Option) *Destination {
	d := &Destination{}
	for _, opt := range opts {
		opt(&d.opts)
	}
	d.client = goredis.NewClient(&d.opts.opts)
	return d
}

type binMarshaler []byte

func (b binMarshaler) MarshalBinary() ([]byte, error) {
	return b, nil
}

func (d *Destination) Send(ctx context.Context, ack func(), msgs ...kawa.Message[[]byte]) error {
	// Send to redis server using XADD
	for _, msg := range msgs {
		_, err := d.client.XAdd(ctx, &goredis.XAddArgs{
			Stream: d.opts.topic,
			Values: map[string]any{"msg": binMarshaler(msg.Value)},
		}).Result()
		if err != nil {
			return err
		}
	}
	kawa.Ack(ack)
	return nil
}

type Source struct {
	opts   Options
	client *goredis.Client
	msgC   chan kawa.MsgAck[[]byte]
}

func NewSource(opts ...Option) *Source {
	d := &Source{
		msgC: make(chan kawa.MsgAck[[]byte]),
	}
	for _, opt := range opts {
		opt(&d.opts)
	}
	d.client = goredis.NewClient(&d.opts.opts)
	return d
}

func (s *Source) Run(ctx context.Context) error {
	return s.recvLoop(ctx)
}

func (s *Source) recvLoop(ctx context.Context) error {
	var err error

	gid := ksuid.New().String()

outer:
	for {
		msgs, err := s.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    "kawa",
			Consumer: gid,
			Streams:  []string{s.opts.topic, ">"},
			Count:    100,
			Block:    0,
		}).Result()
		if err != nil {
			return err
		}

		// read some messages from the topic
		for _, msg := range msgs {
			topic := msg.Stream
			for _, m := range msg.Messages {
				var bts []byte
				if v, ok := m.Values["msg"].(string); ok {
					bts = []byte(v)
				}

				mid := m.ID
				msgAck := kawa.MsgAck[[]byte]{
					Msg: kawa.Message[[]byte]{
						Key:   m.ID,
						Topic: topic,
						Value: bts,
					},
					Ack: func() {
						_, err := s.client.XAck(ctx, topic, "kawa", mid).Result()
						if err != nil {
							slog.Warn("failed to ack redis message", "err", err)
						}
					},
				}

				select {
				case <-ctx.Done():
					err = ctx.Err()
					break outer
				case s.msgC <- msgAck:
				}

			}

		}
	}

	return err
}

func (s *Source) Recv(ctx context.Context) (kawa.Message[[]byte], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[[]byte]{}, nil, ctx.Err()
	case msgAck := <-s.msgC:
		return msgAck.Msg, msgAck.Ack, nil
	}
}
