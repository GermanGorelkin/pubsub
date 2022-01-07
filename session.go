package pubsub

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	ErrNotConnected  = errors.New("not connected to a server")
	ErrAlreadyClosed = errors.New("already closed: not connected to the server")
	ErrShutdown      = errors.New("session is shutting down")
)

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
}

type Bind struct {
	QueueName    string
	ExchangeName string
	Key          string
}

type EventHandler func([]byte) error

type Consumer struct {
	Name      string
	QueueName string
	AutoAck   bool
	Exclusive bool

	Handler EventHandler
	run     int32
}

type Option func(*Session)

func WithDeclare(ex Exchange, q Queue, b Bind) Option {
	return func(s *Session) {
		s.declarationSet.Exchange = ex
		s.declarationSet.Queue = q
		s.declarationSet.Bind = b
		s.declarationSet.isUsageDefault = true
		s.declarationSet.isDecalre = true
	}
}

type Session struct {
	addr string

	declarationSet struct {
		isUsageDefault bool
		isDecalre      bool
		Exchange       Exchange
		Queue          Queue
		Bind           Bind
	}

	consumers []*Consumer
	cond      *sync.Cond
	cond2     *sync.Cond

	connection      *amqp.Connection
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	done            chan bool
	isReady         int32
}

// New creates a new Session instance, and automatically
// attempts to connect to the server.
func New(addr string, opts ...Option) *Session {
	session := &Session{
		addr:    addr,
		isReady: 0,
		done:    make(chan bool),
		cond:    sync.NewCond(&sync.Mutex{}),
		cond2:   sync.NewCond(&sync.Mutex{}),
	}

	for _, opt := range opts {
		opt(session)
	}

	go session.handleReconnect()
	go session.handleReOpenChannel()
	go session.handleConsumers()

	return session
}

func (session *Session) QueueDeclare(q Queue) *Session {
	return session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect() {
	for {
		atomic.StoreInt32(&session.isReady, 0)
		log.Printf("Attempting to connect")

		_, err := session.connect()

		if err != nil {
			log.Printf("Failed to connect. Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		session.cond2.L.Lock()
		session.cond2.Broadcast()
		session.cond2.L.Unlock()

		select {
		case <-session.done:
			return
		case err := <-session.notifyConnClose:
			{
				if err == nil { // expected shutdown
					return
				}
				log.Printf("Connection closed:%v", err)
			}
		}
		log.Printf("Reconnecting...")
	}
}

// connect will create a new AMQP connection
func (session *Session) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(session.addr)
	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	log.Println("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReOpenChannel() {
	for {
		atomic.StoreInt32(&session.isReady, 0)

		session.cond2.L.Lock()

		for session.connection == nil || session.connection.IsClosed() {
			session.cond2.Wait()
			select {
			case <-session.done:
				return
			default:
			}
		}

		err := session.init(session.connection)

		session.cond2.L.Unlock()

		if err != nil {
			log.Printf("Error init:%v", err)
			log.Printf("Failed to initialize channel. Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return
		case err := <-session.notifyChanClose:
			{
				log.Printf("Channel closed:%v", err)
				if err == nil { // expected shutdown
					return
				}
			}
		}
		log.Println("Re-running init...")
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	if conn == nil || conn.IsClosed() {
		return ErrNotConnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	if err := session.declare(ch); err != nil {
		return err
	}

	if err := ch.Confirm(false); err != nil {
		return err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}

	session.changeChannel(ch)

	atomic.StoreInt32(&session.isReady, 1)

	session.cond.L.Lock()
	session.cond.Signal()
	session.cond.L.Unlock()

	log.Printf("Setup!")

	return nil
}

func (session *Session) declare(ch *amqp.Channel) error {
	if !session.declarationSet.isDecalre {
		return nil
	}

	if err := ch.ExchangeDeclare(
		session.declarationSet.Exchange.Name,       // name
		session.declarationSet.Exchange.Kind,       // type
		session.declarationSet.Exchange.Durable,    // durable
		session.declarationSet.Exchange.AutoDelete, // auto-deleted
		session.declarationSet.Exchange.Internal,   // internal
		false,                                      // no-wait
		nil,                                        // arguments
	); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(
		session.declarationSet.Queue.Name,       // name
		session.declarationSet.Queue.Durable,    // durable
		session.declarationSet.Queue.AutoDelete, // delete when unused
		session.declarationSet.Queue.Exclusive,  // exclusive
		false,                                   // no-wait
		nil,                                     // arguments
	); err != nil {
		return err
	}

	if err := ch.QueueBind(
		session.declarationSet.Bind.QueueName,
		session.declarationSet.Bind.Key,
		session.declarationSet.Bind.ExchangeName,
		false,
		nil); err != nil {
		return err
	}

	return nil
}

func (session *Session) handleConsumers() {
	for {
		session.cond.L.Lock()
		session.cond.Wait()

		select {
		case <-session.done:
			return
		default:
		}

		for _, cons := range session.consumers {
			if atomic.LoadInt32(&cons.run) == 0 && atomic.LoadInt32(&session.isReady) == 1 {
				if err := session.handleConsumer(cons); err != nil {
					log.Printf("failed to handleConsumer:%v", err)
				}
			}
		}

		session.cond.L.Unlock()
	}
}

func (session *Session) handleConsumer(cons *Consumer) error {
	deliveries, err := session.Stream(cons)
	if err != nil {
		return err
	}
	atomic.StoreInt32(&cons.run, 1)

	go func() {
		log.Printf("Consumer running!")
		for d := range deliveries {
			select {
			case <-session.done:
				break
			default:
			}

			session.handleDelivery(d, cons.Handler)
		}
		atomic.StoreInt32(&cons.run, 0)
		log.Printf("Consumer closed!")

		session.cond.L.Lock()
		session.cond.Signal()
		session.cond.L.Unlock()
	}()

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (session *Session) changeConnection(connection *amqp.Connection) {
	session.connection = connection
	session.notifyConnClose = make(chan *amqp.Error)
	session.connection.NotifyClose(session.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (session *Session) changeChannel(channel *amqp.Channel) {
	session.channel = channel
	session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	session.channel.NotifyClose(session.notifyChanClose)
	session.channel.NotifyPublish(session.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (session *Session) Push(data []byte) error {
	if atomic.LoadInt32(&session.isReady) == 0 {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := session.UnsafePush(data)
		if err != nil {
			log.Printf("Push failed. Retrying...")
			select {
			case <-session.done:
				return ErrShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-session.notifyConfirm:
			if confirm.Ack {
				//log.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		log.Printf("Push didn't confirm. Retrying...")
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (session *Session) UnsafePush(data []byte) error {
	if atomic.LoadInt32(&session.isReady) == 0 {
		return ErrNotConnected
	}
	if !session.declarationSet.isUsageDefault {
		return fmt.Errorf("Don't set exchange and routing key")
	}
	return session.channel.Publish(
		session.declarationSet.Exchange.Name, // Exchange
		session.declarationSet.Bind.Key,      // Routing key
		false,                                // Mandatory
		false,                                // Immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         data,
		},
	)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (session *Session) Stream(c *Consumer) (<-chan amqp.Delivery, error) {
	if atomic.LoadInt32(&session.isReady) == 0 {
		return nil, ErrNotConnected
	}
	if !session.declarationSet.isUsageDefault {
		return nil, fmt.Errorf("Don't set queue")
	}
	return session.channel.Consume(
		c.QueueName, // Queue
		c.Name,      // Consumer
		c.AutoAck,   // Auto-Ack
		c.Exclusive, // Exclusive
		false,       // No-local
		false,       // No-Wait
		nil,         // Args
	)
}

func (session *Session) Subscribe(handler func([]byte) error) error {
	cons := &Consumer{
		Handler:   handler,
		QueueName: session.declarationSet.Queue.Name,
	}

	session.cond.L.Lock()
	session.consumers = append(session.consumers, cons)
	session.cond.Signal()
	session.cond.L.Unlock()

	return nil
}

func (session *Session) handleDelivery(d amqp.Delivery, handler EventHandler) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("handleDelivery panic:%v", r)
			_ = d.Nack(false, true)
		}
	}()

	if err := handler(d.Body); err == nil {
		_ = d.Ack(false)
	} else {
		log.Printf("Failed to handler msg:%v", err)
		_ = d.Nack(false, true)
	}
}

// Close will cleanly shutdown the channel and connection.
func (session *Session) Close() error {
	if !atomic.CompareAndSwapInt32(&session.isReady, 1, 0) {
		return ErrAlreadyClosed
	}

	err := session.channel.Close()
	if err != nil {
		return err
	}
	err = session.connection.Close()
	if err != nil {
		return err
	}
	close(session.done)
	session.cond.Broadcast()
	session.cond2.Broadcast()
	log.Printf("Session closed!")
	return nil
}

func (session *Session) ChannelClose() error {
	return session.channel.Close()
}
