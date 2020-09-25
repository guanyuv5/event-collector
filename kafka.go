package main

import (
	"fmt"

	"github.com/Shopify/sarama"
)

//KafkaOps xx
type KafkaOps struct {
	KafkaClient sarama.SyncProducer
	Topic       string
}

//NewKafkaClient xx
func NewKafkaClient(broker string, topic string) (*KafkaOps, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	kafkaClient, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaOps{
		KafkaClient: kafkaClient,
		Topic:       topic,
	}, nil
}

//Producer xx
func (k *KafkaOps) Producer(e string) {
	fmt.Println("send: ", e)
	msg := &sarama.ProducerMessage{}
	msg.Topic = k.Topic
	msg.Value = sarama.StringEncoder(e)
	pid, offset, err := k.KafkaClient.SendMessage(msg)
	if err != nil {
		fmt.Println("send message failed,", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)
}
