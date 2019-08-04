package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"strconv"
	"sync"
	"time"
)

const (
	//mqurl ="amqp://guest:guest@172.16.204.11:5672"
	mqurl = "amqp://guest:guest@10.130.121.134:30773"
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
	var intervalTime int
	flag.IntVar(&intervalTime, "intervalTime", 500, "")

	flag.Parse()
	fmt.Println("queueCount:", queueCount)
	fmt.Println("consumerCount:", consumerCount)
	fmt.Println("prodNum:", prodNum)
	fmt.Println("intervalTime:", intervalTime)

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
				fmt.Println("publisher queue id: " + strconv.Itoa(k) + "  " + queues[k] )
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
				time.Sleep(time.Duration(intervalTime)*time.Millisecond)
			}
		}(i)
	}

	// consumers
	for i:=0; i< consumerCount; i++ {
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
			queueName := "queue" +  strconv.Itoa(k)
			fmt.Println("consumer queue id: " + strconv.Itoa(k) + "  " + queueName )
			msgs, _ := channel.Consume(queueName, "", false, false, false, false, nil)
			go func() {
				for d := range msgs {
					s := BytesToString(&(d.Body))
					d.Ack(false)
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
