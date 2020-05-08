package main

import (
        "fmt"
	"strings"

	"github.com/Shopify/sarama"

        kafka "github.com/hongbinbao/go-mod-project-skelton/wheel"
        auth  "github.com/hongbinbao/go-mod-project-skelton/pkg/auth"
)


func testAsyncKafkaProducter() {
	brokers := "a.b.c.d:9092,a.b.c.d:9093"
	brokerList := strings.Split(brokers, ",")
	producter := kafka.NewAsyncProducer(brokerList)
	message := &sarama.ProducerMessage{Topic: "testtopic2019", Value: sarama.StringEncoder("testing async producter")}
	producter.Input() <- message
}

func main() {
        fmt.Println(auth.NewAuthManager())
	testAsyncKafkaProducter()
}
