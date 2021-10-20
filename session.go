package rabbitmq_session

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
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

type EventHandler func([]byte) error

type Consumer struct {
	handler EventHandler
	run     int32
}

type Session struct {
	exchangeName string
	exchangeType string
	queueName    string
	bindingKey   string
	consumerTag  string
	consumers    []*Consumer
	cond         *sync.Cond

	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	// notifyChanClose chan *amqp.Error
	notifyConfirm chan amqp.Confirmation
	isReady       int32

	logger *zap.SugaredLogger
}

type SessionConfig struct {
	ExchangeName string
	ExchangeType string
	QueueName    string
	BindingKey   string
	Addr         string

	Logger *zap.SugaredLogger
}

// New creates a new Session instance, and automatically
// attempts to connect to the server.
func New(cfg SessionConfig) *Session {
	session := Session{
		exchangeName: cfg.ExchangeName,
		exchangeType: cfg.ExchangeType,
		queueName:    cfg.QueueName,
		bindingKey:   cfg.BindingKey,
		consumerTag:  "",
		isReady:      0,
		done:         make(chan bool),

		cond: sync.NewCond(&sync.Mutex{}),

		logger: cfg.Logger,
	}
	go session.handleReconnect(cfg.Addr)
	go session.runConsumers()
	return &session
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (session *Session) handleReconnect(addr string) {
	for {
		atomic.StoreInt32(&session.isReady, 0)
		session.logger.Info("Attempting to connect")

		conn, err := session.connect(addr)

		if err != nil {
			session.logger.Info("Failed to connect. Retrying...")

			select {
			case <-session.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}

		if done := session.handleReInit(conn); done {
			break
		}
	}
}

// connect will create a new AMQP connection
func (session *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	session.changeConnection(conn)
	session.logger.Info("Connected!")
	return conn, nil
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (session *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		atomic.StoreInt32(&session.isReady, 0)

		err := session.init(conn)

		if err != nil {
			session.logger.Errorf("Error init:%v", err)
			session.logger.Info("Failed to initialize channel. Retrying...")

			select {
			case <-session.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-session.done:
			return true
		case err := <-session.notifyConnClose:
			{
				if err == nil {
					return true
				}
				session.logger.Info("Connection closed. Reconnecting...")
				return false
			}
			// case <-session.notifyChanClose:
			// 	session.logger.Info("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare queue
func (session *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		session.exchangeName, // name
		session.exchangeType, // type
		false,                // durable
		false,                // auto-deleted
		false,                // internal
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		session.queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		session.queueName,
		session.bindingKey,
		session.exchangeName,
		false,
		nil)
	if err != nil {
		return err
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return err
	}

	session.changeChannel(ch)

	atomic.StoreInt32(&session.isReady, 1)

	session.cond.L.Lock()
	session.cond.Signal()
	session.cond.L.Unlock()

	session.logger.Info("Setup!")

	return nil
}

func (session *Session) runConsumers() {
	for {
		session.cond.L.Lock()
		session.cond.Wait()

		select {
		case <-session.done:
			return
		default:
		}

		for _, cons := range session.consumers {
			if atomic.CompareAndSwapInt32(&cons.run, 0, 1) {
				if err := session.runConsumer(cons); err != nil {
					atomic.StoreInt32(&cons.run, 0)
					session.logger.Errorf("failed to runConsumer:%v", err)
				}
			}
		}

		session.cond.L.Unlock()
	}
}

func (session *Session) runConsumer(cons *Consumer) error {
	deliveries, err := session.Stream()
	if err != nil {
		return err
	}

	go func() {
		session.logger.Info("Consumer init!")
		for d := range deliveries {
			select {
			case <-session.done:
				break
			default:
			}

			session.handleDelivery(d, cons.handler)
		}
		atomic.StoreInt32(&cons.run, 0)
		session.logger.Info("Consumer closed!")

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
	// session.notifyChanClose = make(chan *amqp.Error)
	session.notifyConfirm = make(chan amqp.Confirmation, 1)
	// session.channel.NotifyClose(session.notifyChanClose)
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
			//session.log.Info("Push failed. Retrying...")
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
		//log.Println("Push didn't confirm. Retrying...")
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
	return session.channel.Publish(
		session.exchangeName, // Exchange
		session.bindingKey,   // Routing key
		false,                // Mandatory
		false,                // Immediate
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
func (session *Session) Stream() (<-chan amqp.Delivery, error) {
	if atomic.LoadInt32(&session.isReady) == 0 {
		return nil, ErrNotConnected
	}
	return session.channel.Consume(
		session.queueName,
		session.consumerTag, // Consumer
		false,               // Auto-Ack
		false,               // Exclusive
		false,               // No-local
		false,               // No-Wait
		nil,                 // Args
	)
}

func (session *Session) Subscribe(handler func([]byte) error) error {
	cons := &Consumer{handler: handler}

	session.cond.L.Lock()
	session.consumers = append(session.consumers, cons)
	session.cond.Signal()
	session.cond.L.Unlock()

	return nil
}

func (session *Session) handleDelivery(d amqp.Delivery, handler EventHandler) {
	defer func() {
		if r := recover(); r != nil {
			session.logger.Infof("handleDelivery panic:%v", r)
			_ = d.Nack(false, true)
		}
	}()

	if err := handler(d.Body); err == nil {
		_ = d.Ack(false)
	} else {
		session.logger.Errorf("failed to handler msg:%v", err)
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
	return nil
}
