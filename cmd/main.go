package main

import (
	"flag"
	"fmt"
	"bytes"
	"strconv"
	"sync"
	"github.com/streadway/amqp"
	"time"
)

const (
	//mqurl ="amqp://guest:guest@172.16.204.11:5672"
	mqurl = "amqp://guest:guest@10.130.121.134:30773"
	workerNum = 10
	factor = 20
)

func main() {
	defer func(){
		if err:=recover();err!=nil{
			fmt.Println(err)
		}
	}()

	var queueCount int
	flag.IntVar(&queueCount, "queueCount", 100, "")
	var consumerCount int
	flag.IntVar(&consumerCount, "consumerCount", 1000, "")
	var prodNum int
	flag.IntVar(&prodNum, "prodNum", 10000, "")

	flag.Parse()
	fmt.Println("queueCount:", queueCount)
	fmt.Println("consumerCount:", consumerCount)
	fmt.Println("prodNum:", prodNum)

	queues := make([]string, queueCount)
	var waitGroutp = sync.WaitGroup{}

	var conn *amqp.Connection
	var channel *amqp.Channel
	conn, _ = amqp.Dial(mqurl)
	channel, _ = conn.Channel()

	// queues
	for i:=0; i< factor; i++ {
		go func(i int) {
			startIndex := i * (queueCount/factor)
			endIndex := (i + 1) * (queueCount/factor)
			fmt.Println("queue startIndex endIndex" + strconv.Itoa(startIndex) + "  " + strconv.Itoa(endIndex) )
			for k:=startIndex; k< endIndex; k++ {
				channel.QueueDeclare("queue" + strconv.Itoa(k), false, false, false, false, nil)
				queues[k] = "queue" + strconv.Itoa(k)
			}
			waitGroutp.Done()
		}(i)
	}

	waitGroutp.Add(factor)
	waitGroutp.Wait()

	// print queue list
	for i:=0; i< queueCount; i++ {
		fmt.Println(queues[i])
	}

	// publishers
	for i:=0; i< prodNum; i++ {
		go func(i int) {
			var conn *amqp.Connection
			var channel *amqp.Channel
			for {
				conn, _ = amqp.Dial(mqurl)
				if conn != nil {
					break
				}
			}
			for {
				channel, _ = conn.Channel()
				if channel != nil {
					break
				}
			}
			k := i % queueCount
			for {
				fmt.Println("publisher queue id: " + strconv.Itoa(k) + "  " + queues[k] )
				msgContent := "test message"
				channel.Publish("", queues[k], false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(msgContent),
				})
			}
		}(i)
	}

	// consumers
	for i:=0; i< consumerCount; i++ {
		go func(i int) {
			k := i % queueCount
			var conn *amqp.Connection
			var channel *amqp.Channel
			for {
				conn, _ = amqp.Dial(mqurl)
				if conn != nil {
					break
				}
			}
			for {
				channel, _ = conn.Channel()
				if channel != nil {
					break
				}
			}
			queueName := "queue" +  strconv.Itoa(k)
			fmt.Println("consumer queue id: " + strconv.Itoa(k) + "  " + queueName )
			msgs, _ := channel.Consume(queueName, "", true, false, false, false, nil)
			go func() {
				for d := range msgs {
					s := BytesToString(&(d.Body))
					fmt.Printf("receve msg is :%s\n", *s)
					time.Sleep(time.Duration(1)*time.Second)
				}
			}()
		}(i)
	}

	forever := make(chan bool)
	fmt.Printf("To exit press CTRL+C\n")
	<-forever
}

func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}
