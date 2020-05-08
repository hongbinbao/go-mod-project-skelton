package wheel


import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"

)

const (
	//exit with status 1 if failed to start Sarama producer: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)
	BrokerList string = "a.b.c.d:9092,a.b.c.d:9093" //The Kafka brokers to connect to, as a comma separated list
	certFile   string = ""                                        //The optional certificate file for client authentication
	keyFile    string = ""                                        //The optional key file for client authentication
	caFile     string = ""                                        //The optional certificate authority file for TLS client authentication
	verifySsl  bool   = false
)

func TestKafkaInit() {
	brokerList := strings.Split(BrokerList, ",")
	log.Println(brokerList)
	dc := newSyncCollector(brokerList)
	partition, offset, err := dc.SendMessage(&sarama.ProducerMessage{
		Topic: "test topic 2020",
		Value: sarama.StringEncoder("value1"),
	})
	log.Println("partition", partition)
	if err != nil {
		panic(err)
	}
	log.Println(partition, offset) //./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic testtopic2019

}

func createTlsConfiguration() (t *tls.Config) {
	if certFile != "" && keyFile != "" && caFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}

/**Sync**/
func newSyncCollector(brokerList []string) sarama.SyncProducer {

	// For the data collector, we are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Enable = true
	}

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	return producer
}

/**Async**/
func NewAsyncProducer(brokerList []string) sarama.AsyncProducer {

	// For the access log, we are looking for AP semantics, with high throughput.
	// By creating batches of compressed messages, we reduce network I/O at a cost of more latency.
	config := sarama.NewConfig()
	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}

