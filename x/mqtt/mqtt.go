package mqtt

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/runreveal/kawa"
)

type OptFunc func(*Opts)

type Opts struct {
	broker   string
	clientID string
	topic    string

	userName string
	password string

	qos      byte
	retained bool
}

func WithBroker(broker string) func(*Opts) {
	return func(opts *Opts) {
		opts.broker = broker
	}
}

func WithClientID(clientID string) func(*Opts) {
	return func(opts *Opts) {
		opts.clientID = clientID
	}
}

func WithTopic(topic string) func(*Opts) {
	return func(opts *Opts) {
		if topic == "" {
			opts.topic = "#"
		} else {
			opts.topic = topic
		}
	}
}

func WithQOS(qos byte) func(*Opts) {
	return func(opts *Opts) {
		opts.qos = qos
	}
}

func WithRetained(retained bool) func(*Opts) {
	return func(opts *Opts) {
		opts.retained = retained
	}
}

func WithUserName(userName string) func(*Opts) {
	return func(opts *Opts) {
		opts.userName = userName
	}
}

func WithPassword(password string) func(*Opts) {
	return func(opts *Opts) {
		opts.password = password
	}
}

type Destination struct {
	client MQTT.Client
	cfg    Opts
}

type Source struct {
	msgC chan msgAck
	cfg  Opts
}

type msgAck struct {
	msg kawa.Message[[]byte]
	ack func()
}

func loadOpts(opts []OptFunc) Opts {
	cfg := Opts{
		topic:    "#",
		retained: false,
		qos:      1,
	}

	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func NewSource(opts ...OptFunc) *Source {
	cfg := loadOpts(opts)

	ret := &Source{
		msgC: make(chan msgAck),
		cfg:  cfg,
	}

	return ret
}

func NewDestination(opts ...OptFunc) *Destination {
	cfg := loadOpts(opts)
	ret := &Destination{
		cfg: cfg,
	}

	return ret
}

func clientConnect(opts Opts, onLost MQTT.ConnectionLostHandler) (MQTT.Client, error) {

	if opts.broker == "" {
		return nil, errors.New("mqtt: missing broker")
	}
	if opts.clientID == "" {
		return nil, errors.New("mqtt: missing clientID")
	}

	clientOpts := MQTT.NewClientOptions().AddBroker(opts.broker).SetClientID(opts.clientID).SetConnectionLostHandler(onLost)

	if opts.userName != "" {
		clientOpts = clientOpts.SetUsername(opts.userName)
	}
	if opts.password != "" {
		clientOpts = clientOpts.SetPassword(opts.password)
	}

	client := MQTT.NewClient(clientOpts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("mqtt connect error: %s", token.Error())
	}

	return client, nil
}

func (dest *Destination) Run(ctx context.Context) error {
	var err error
	errc := make(chan error)

	connLost := func(client MQTT.Client, err error) {
		errc <- err
	}

	dest.client, err = clientConnect(dest.cfg, connLost)
	if err != nil {
		return err
	}

loop:
	for {
		select {
		case err = <-errc:
			break loop
		case <-ctx.Done():
			err = ctx.Err()
			break loop
		}
	}

	dest.client.Disconnect(1000)
	return err
}

func (dest *Destination) Send(ctx context.Context, ack func(), msgs ...kawa.Message[[]byte]) error {
	for _, msg := range msgs {

		token := dest.client.Publish(dest.cfg.topic, dest.cfg.qos, dest.cfg.retained, string(msg.Value))
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}
	}
	return nil
}

func (src *Source) Run(ctx context.Context) error {
	return src.recvLoop(ctx)
}

func (src *Source) recvLoop(ctx context.Context) error {
	errc := make(chan error)

	newMessage := func(client MQTT.Client, message MQTT.Message) {
		select {
		case src.msgC <- msgAck{
			msg: kawa.Message[[]byte]{
				Value: message.Payload(),
				Key:   strconv.FormatUint(uint64(message.MessageID()), 10),
				Topic: message.Topic(),
			},
			ack: message.Ack,
		}:
		case <-ctx.Done():
			return
		}
	}

	connLost := func(client MQTT.Client, err error) {
		errc <- err
	}

	client, err := clientConnect(src.cfg, connLost)
	if err != nil {
		return err
	}

	token := client.Subscribe(src.cfg.topic, src.cfg.qos, newMessage)
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("mqtt subscribe error: %s", token.Error())
	}

	defer client.Unsubscribe(src.cfg.topic)
	defer client.Disconnect(250)

	for {
		select {
		// case <-time.After(60 * time.Second):
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (src *Source) Recv(ctx context.Context) (kawa.Message[[]byte], func(), error) {
	select {
	case <-ctx.Done():
		return kawa.Message[[]byte]{}, nil, ctx.Err()
	case pass := <-src.msgC:
		return pass.msg, pass.ack, nil
	}
}
