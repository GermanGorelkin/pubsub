package rabbitmq_session

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"

	session "github.com/germangorelkin/rabbitmq-session"
)

var opts = &dockertest.RunOptions{
	Repository:   "rabbitmq",
	Tag:          "3.9-management-alpine",
	ExposedPorts: []string{"5672"},
	PortBindings: map[docker.Port][]docker.PortBinding{"5672": {{HostIP: "0.0.0.0", HostPort: "5672"}}},
}

func TestOpenClose(t *testing.T) {

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.Println("connect to docker")

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(opts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	defer pool.Purge(resource)
	log.Printf("run container id=%s", resource.Container.ID)

	session := session.New("amqp://guest:guest@localhost:5672/",
		session.WithDeclare(
			session.Exchange{Name: "exchange_test", Kind: "topic"},
			session.Queue{Name: "queue_test"},
			session.Bind{QueueName: "queue_test", ExchangeName: "exchange_test", Key: "test"}),
	)
	defer session.Close()

	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := session.Push(message); err == nil {
				i++
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	if err := session.Subscribe(func(b []byte) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("failed to Subscribe")
	}

	if err := session.Subscribe(func(b []byte) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("failed to Subscribe")
	}

	// TODO wait with timeout
	wg.Wait()

	assert.Equal(t, numOfMsg, int(receivedMsg))
}

func TestOpenClose2(t *testing.T) {

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.Println("connect to docker")

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(opts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer pool.Purge(resource)
	log.Printf("run container id=%s", resource.Container.ID)

	session := session.New("amqp://guest:guest@localhost:5672/",
		session.WithDeclare(
			session.Exchange{Name: "exchange_test", Kind: "topic"},
			session.Queue{Name: "queue_test"},
			session.Bind{QueueName: "queue_test", ExchangeName: "exchange_test", Key: "test"}),
	)
	defer session.Close()

	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := session.Push(message); err == nil {
				i++

				if i == numOfMsg/2 {
					log.Println("container stoping")
					if err := pool.Client.StopContainer(resource.Container.ID, 0); err != nil {
						log.Printf("failed to stop container id=%s:%v", resource.Container.ID, err)
					}

					time.Sleep(1 * time.Second)

					log.Println("container starting")
					if err := pool.Client.StartContainer(resource.Container.ID, nil); err != nil {
						log.Printf("failed to start container id=%s:%v", resource.Container.ID, err)
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	if err := session.Subscribe(func(b []byte) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("failed to Subscribe")
	}

	if err := session.Subscribe(func(b []byte) error {
		atomic.AddUint64(&receivedMsg, 1)
		wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("failed to Subscribe")
	}

	wg.Wait()

	assert.Equal(t, numOfMsg, int(receivedMsg))
}

func TestOpenClose3(t *testing.T) {

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	log.Println("connect to docker")

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(opts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer pool.Purge(resource)
	log.Printf("run container id=%s", resource.Container.ID)

	session := session.New("amqp://guest:guest@localhost:5672/",
		session.WithDeclare(
			session.Exchange{Name: "exchange_test", Kind: "topic"},
			session.Queue{Name: "queue_test", Durable: true},
			session.Bind{QueueName: "queue_test", ExchangeName: "exchange_test", Key: "test"}),
	)
	defer session.Close()

	numOfMsg := 100

	go func() {
		for i := 0; i < numOfMsg; {
			message := []byte(fmt.Sprintf("message %d", i))
			if err := session.Push(message); err == nil {
				i++
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(numOfMsg)
	var receivedMsg uint64

	if err := session.Subscribe(func(b []byte) error {
		atomic.AddUint64(&receivedMsg, 1)

		if atomic.LoadUint64(&receivedMsg) == uint64(numOfMsg/2) {
			log.Println("container stoping")
			if err := pool.Client.StopContainer(resource.Container.ID, 0); err != nil {
				log.Printf("failed to stop container id=%s:%v", resource.Container.ID, err)
			}

			time.Sleep(1 * time.Second)

			log.Println("container starting")
			if err := pool.Client.StartContainer(resource.Container.ID, nil); err != nil {
				log.Printf("failed to start container id=%s:%v", resource.Container.ID, err)
			}
		}

		wg.Done()
		return nil
	}); err != nil {
		t.Fatalf("failed to Subscribe")
	}

	wg.Wait()

	assert.Equal(t, numOfMsg, int(receivedMsg))
}
