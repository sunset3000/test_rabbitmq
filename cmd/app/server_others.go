package app

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"strconv"
	"sync"
	"time"

	"k8s.io/klog"
)

// TestClient represents all the parameters required to start the test client. All
// fields are required.
type TestClient struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel

	mqURL string

	queueCount int

	prodNum int

	consumerCount int

	intervalTime int
}

// NewProxyServer returns a new ProxyServer.
func NewTestClient(o *Options) (*TestClient, error) {
	return newTestClient(o)
}

func newTestClient(o *Options) (*TestClient, error) {
	var conn *amqp.Connection
	var channel *amqp.Channel
	var err error
	conn, err = amqp.Dial(o.mqURL)
	if err != nil {
		fmt.Printf("%v", err)
		return nil, err
	}

	channel, err = conn.Channel()
	if err != nil {
		fmt.Printf("%v", err)
		return nil, err
	}

	return &TestClient{
		Conn:          conn,
		Channel:       channel,
		mqURL:         o.mqURL,
		queueCount:    o.queueCount,
		prodNum:       o.prodNum,
		consumerCount: o.consumerCount,
		intervalTime:  o.intervalTime,
	}, nil
}

func (c *TestClient) Run() error {
	klog.Info("TestClient is running")

	channel := c.Channel
	factor := 10
	queueCount := c.queueCount
	prodNum := c.prodNum
	mqurl := c.mqURL
	intervalTime := c.intervalTime
	consumerCount := c.consumerCount

	queues := make([]string, queueCount)
	var waitGroup = sync.WaitGroup{}

	// queues
	for i := 0; i < factor; i++ {
		go func(i int) {
			startIndex := i * (queueCount / factor)
			endIndex := (i + 1) * (queueCount / factor)
			fmt.Println("queue startIndex endIndex" + strconv.Itoa(startIndex) + "  " + strconv.Itoa(endIndex))
			for k := startIndex; k < endIndex; k++ {
				_, err := channel.QueueDeclare("queue"+strconv.Itoa(k), false, false, false, false, nil)
				if err != nil {
					fmt.Printf("error: %v", err)
					return
				}
				queues[k] = "queue" + strconv.Itoa(k)
			}
			waitGroup.Done()
		}(i)
	}

	waitGroup.Add(factor)
	waitGroup.Wait()

	// print queue list
	for i := 0; i < queueCount; i++ {
		fmt.Println(queues[i])
	}

	// publishers
	for i := 0; i < prodNum; i++ {
		go func(i int) {
			var conn *amqp.Connection
			var channel *amqp.Channel
			conn, err := amqp.Dial(mqurl)
			if err != nil {
				fmt.Println(err)
				return
			}
			channel, err = conn.Channel()
			if err != nil {
				fmt.Println(err)
				return
			}
			k := i % queueCount
			for {
				fmt.Println("publisher queue id: " + strconv.Itoa(k) + "  " + queues[k])
				msgContent := "test messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest messagetest message"
				err = channel.Publish("", queues[k], false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(msgContent),
				})
				if err != nil {
					fmt.Println(err)
					conn, err := amqp.Dial(mqurl)
					if err != nil {
						fmt.Println(err)
						return
					}
					channel, err = conn.Channel()
					if err != nil {
						fmt.Println(err)
						return
					}
				}
				time.Sleep(time.Duration(intervalTime) * time.Millisecond)
			}
		}(i)
	}

	// consumers
	for i := 0; i < consumerCount; i++ {
		go func(i int) {
			k := i % queueCount
			var conn *amqp.Connection
			var channel *amqp.Channel
			conn, err := amqp.Dial(mqurl)
			if err != nil {
				fmt.Println(err)
				return
			}
			channel, err = conn.Channel()
			if err != nil {
				fmt.Println(err)
				return
			}
			queueName := "queue" + strconv.Itoa(k)
			fmt.Println("consumer queue id: " + strconv.Itoa(k) + "  " + queueName)
			msgs, _ := channel.Consume(queueName, "", true, false, false, false, nil)
			go func() {
				for d := range msgs {
					s := BytesToString(&(d.Body))
					//d.Ack(false)
					fmt.Printf("receve msg is :%s\n", *s)
				}
			}()
		}(i)
	}

	return nil
}

func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}

func (c *TestClient) CleanupAndExit() error {
	klog.Info("TestClient start to cleanup")

	klog.Info("TestClient exit")
	return nil
}
