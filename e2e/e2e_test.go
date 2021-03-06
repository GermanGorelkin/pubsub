package e2e

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/suite"

	pubsub "github.com/germangorelkin/pubsub"
)

func TestRabbitmqSessionSuite(t *testing.T) {
	suite.Run(t, new(RabbitmqSessionSuite))
}

type RabbitmqSessionSuite struct {
	suite.Suite

	resource *dockertest.Resource
	pool     *dockertest.Pool
	opts     *dockertest.RunOptions

	session *pubsub.Session
}

func (s *RabbitmqSessionSuite) SetupSuite() {
	s.opts = &dockertest.RunOptions{
		Repository:   "rabbitmq",
		Tag:          "3.9-alpine",
		ExposedPorts: []string{"5672", "15672"},
		PortBindings: map[docker.Port][]docker.PortBinding{"5672": {{HostIP: "0.0.0.0", HostPort: "5672"}}, "15672": {{HostIP: "0.0.0.0", HostPort: "15672"}}},
		Name:         "rebbitmq-e2e-for-session-ZReqe13s4lg",
	}

	pool, err := dockertest.NewPool("")
	s.Require().Nilf(err, "failed to connect to docker")
	s.pool = pool
}

// func (s *RabbitmqSessionSuite) TearDownSuite() {}

func (s *RabbitmqSessionSuite) SetupTest() {
	// stop container if it's already running
	if cont, ok := s.pool.ContainerByName(s.opts.Name); ok {
		cont.Close()
	}

	// runnig container
	resource, err := s.pool.RunWithOptions(s.opts)
	s.Require().Nilf(err, "failed to run container")
	s.resource = resource
	// log.Printf("container id=%s is running", s.resource.Container.ID)

	// new session
	s.session = pubsub.New("amqp://guest:guest@localhost:5672/",
		pubsub.WithDeclare(
			pubsub.Exchange{Name: "exchange_test", Kind: "topic", IsUsageDefault: true},
			pubsub.Queue{Name: "queue_test", Durable: true, IsUsageDefault: true},
			pubsub.Bind{QueueName: "queue_test", ExchangeName: "exchange_test", Key: "test", IsUsageDefault: true}),
	)
}

func (s *RabbitmqSessionSuite) TearDownTest() {
	s.session.Close()

	if s.resource != nil {
		log.Printf("container id=%s is stopping", s.resource.Container.ID)
		s.resource.Close()
	}
}

func (s *RabbitmqSessionSuite) Test_onePublisher_twoSubscribe() {
	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := s.session.Publish(message); err == nil {
				i++
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	err := s.session.Subscribe(func(d pubsub.Delivery) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	})
	s.Require().Nilf(err, "failed to Subscribe1")

	err = s.session.Subscribe(func(d pubsub.Delivery) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	})
	s.Require().Nilf(err, "failed to Subscribe2")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = waitWithCtx(ctx, &wg)
	s.Require().Nilf(err, "timedout waiting for wait group")

	s.Equal(numOfMsg, int(receivedMsg))
}

func (s *RabbitmqSessionSuite) Test_reconnect_whilePublishingMsgs() {
	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := s.session.Publish(message); err == nil {
				i++

				if i == numOfMsg/2 {
					// log.Println("container stoping")
					err := s.pool.Client.StopContainer(s.resource.Container.ID, 0)
					s.Require().Nilf(err, "failed to stop container id=%s", s.resource.Container.ID)

					time.Sleep(1 * time.Second)

					// log.Println("container starting")
					err = s.pool.Client.StartContainer(s.resource.Container.ID, nil)
					s.Require().Nilf(err, "failed to start container id=%s", s.resource.Container.ID)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	err := s.session.Subscribe(func(d pubsub.Delivery) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	})
	s.Require().Nilf(err, "failed to Subscribe1")

	err = s.session.Subscribe(func(d pubsub.Delivery) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	})
	s.Require().Nilf(err, "failed to Subscribe2")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = waitWithCtx(ctx, &wg)
	s.Require().Nilf(err, "timedout waiting for wait group")

	s.Equal(numOfMsg, int(receivedMsg))
}

func (s *RabbitmqSessionSuite) Test_reconnect_whileReceivingMsgs() {
	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := s.session.Publish(message); err == nil {
				i++
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	err := s.session.Subscribe(func(d pubsub.Delivery) error {
		atomic.AddUint64(&receivedMsg, 1)

		if atomic.LoadUint64(&receivedMsg) == uint64(numOfMsg/2) {
			// log.Println("container stoping")
			err := s.pool.Client.StopContainer(s.resource.Container.ID, 0)
			s.Require().Nilf(err, "failed to stop container id=%s", s.resource.Container.ID)

			time.Sleep(1 * time.Second)

			// log.Println("container starting")
			err = s.pool.Client.StartContainer(s.resource.Container.ID, nil)
			s.Require().Nilf(err, "failed to start container id=%s", s.resource.Container.ID)
		}

		wg.Done()
		return nil
	})
	s.Require().Nilf(err, "failed to Subscribe")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = waitWithCtx(ctx, &wg)
	s.Require().Nilf(err, "timedout waiting for wait group")

	s.Equal(numOfMsg, int(receivedMsg))
}

func waitWithCtx(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
