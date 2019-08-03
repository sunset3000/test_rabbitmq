package main

import (
	"fmt"
	"bytes"
	"strconv"
	"sync"
	"github.com/streadway/amqp"
)

const (
	mqurl ="amqp://admin:123456@172.16.204.11:5672"
	queueCount = 100
	consumerCount = 20000
	workerNum = 10
	prodNum = 1000
	factor = 20
)

var queues [queueCount] string
var waitGroutp = sync.WaitGroup{}

func main() {
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
			for {
				k := int(i / queueCount)
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
			k := int(i / queueCount)
			var conn *amqp.Connection
			var channel *amqp.Channel
			conn, _ = amqp.Dial(mqurl)
			channel, _ = conn.Channel()
			queueName := "queue" +  strconv.Itoa(k%queueCount)
			fmt.Println("consumer queue id: " + strconv.Itoa(k) + "  " + queueName )
			msgs, _ := channel.Consume(queueName, "", true, false, false, false, nil)
			go func() {
				for d := range msgs {
					s := BytesToString(&(d.Body))
					fmt.Printf("receve msg is :%s\n", *s)
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
