package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

var wg sync.WaitGroup

func main() {
	fmt.Println("send message")
	sendKafka()

	time.Sleep(5 * time.Second)
	fmt.Println("receive message")
	receiveKafka()
}

func sendKafka() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	//新建一个同步生产者
	client, err := sarama.NewSyncProducer([]string{"106.13.173.237:9092"}, config)
	if err != nil {
		fmt.Println("New producer error:", err)
		return
	}
	defer client.Close()

	//定义一个生产消息，包括topic,消息内容
	msg := &sarama.ProducerMessage{}
	msg.Topic = "topic2"
	msg.Key = sarama.StringEncoder("miles")
	msg.Value = sarama.StringEncoder("hello my first kafka message")

	//发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send message failed,", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)

}

func receiveKafka() {
	version, err := sarama.ParseKafkaVersion("2.3.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config := sarama.NewConfig()
	config.Version = version
	consumer, err := sarama.NewConsumer([]string{"106.13.173.237:9092"}, config)
	if err != nil {
		fmt.Println("consumer connect failed.", err)
		return
	}
	fmt.Println("connect success...")
	defer consumer.Close()

	partitions, err := consumer.Partitions("topic2")
	if err != nil {
		fmt.Println("get partitions failed.err:", err)
		return
	}
	for _, p := range partitions {
		partitionsConsumer, err := consumer.ConsumePartition("topic1", p, sarama.OffsetOldest)
		if err != nil {
			fmt.Println("partitionConsumer error:", err)
			return
		}
		wg.Add(1)
		go func() {
			for m := range partitionsConsumer.Messages() {
				fmt.Printf("key:%s,text:%s,offset:%d\n", string(m.Key), string(m.Value), m.Offset)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
