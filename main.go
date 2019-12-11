package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	//新建一个同步生产者
	client, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("New producer error:", err)
		return
	}
	defer client.AsyncClose()

	//定义一个生产消息，包括topic,消息内容
	msg := &sarama.ProducerMessage{}
	msg.Topic = "topic1"
	msg.Key = sarama.StringEncoder("miles")
	msg.Value = sarama.StringEncoder("hello my first kafka message")

	//发送消息
	client.Input() <- msg
	select {
	case suc := <-client.Successes():
		fmt.Println("offset: ", suc.Offset, "timestamp: ", suc.Timestamp.String(), "partitions: ", suc.Partition)
	case fail := <-client.Errors():
		fmt.Println("err:", fail.Err)
	}

}
