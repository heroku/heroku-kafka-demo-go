package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gin-gonic/gin"
	"github.com/joeshaw/envdecode"
)

type AppConfig struct {
	Kafka struct {
		URL           string `env:"KAFKA_URL,required"`
		TrustedCert   string `env:"KAFKA_TRUSTED_CERT,required"`
		ClientCertKey string `env:"KAFKA_CLIENT_CERT_KEY,required"`
		ClientCert    string `env:"KAFKA_CLIENT_CERT,required"`
		Prefix        string `env:"KAFKA_PREFIX"`
		Topic         string `env:"KAFKA_TOPIC,default=messages"`
		ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP,default=heroku-kafka-demo-go"`
	}

	Web struct {
		Port string `env:"PORT,required"`
	}
}

type KafkaClient struct {
	producer sarama.AsyncProducer
	consumer *cluster.Consumer

	ml               sync.RWMutex
	receivedMessages []Message
}

type Message struct {
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Value     string          `json:"value"`
	Metadata  MessageMetadata `json:"metadata"`
}

type MessageMetadata struct {
	ReceivedAt time.Time `json:"received_at"`
}

var topic string

func main() {
	appconfig := AppConfig{}
	envdecode.MustDecode(&appconfig)

	client := newKafkaClient(&appconfig)
	client.receivedMessages = make([]Message, 0)

	topic = appconfig.topic()

	go client.consumeMessages()
	defer client.producer.Close()
	defer client.consumer.Close()

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	router.GET("/", indexGET)
	router.GET("/messages", client.messagesGET)
	router.POST("/messages", client.messagesPOST)

	router.Run(":" + appconfig.Web.Port)
}

func indexGET(c *gin.Context) {
	h := gin.H{"baseurl": "https://" + c.Request.Host}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}

// This endpoint accesses in memory state gathered
// by the consumer, which holds the last 10 messages received
func (kc *KafkaClient) messagesGET(c *gin.Context) {
	kc.ml.RLock()
	defer kc.ml.RUnlock()
	c.JSON(http.StatusOK, kc.receivedMessages)

}

// A sample producer endpoint.
// It receives messages as http bodies on /messages,
// and posts them directly to a Kafka topic.
func (kc *KafkaClient) messagesPOST(c *gin.Context) {
	message, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Fatal(err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(c.Request.RemoteAddr),
		Value: sarama.ByteEncoder(message),
	}

	kc.producer.Input() <- msg
}

// Consume messages from the topic and buffer the last 10 messages
// in memory so that the web app can send them back over the API.
func (kc *KafkaClient) consumeMessages() {
	for {
		select {
		case message := <-kc.consumer.Messages():
			kc.saveMessage(message)
		}
	}
}

// Save consumed message in the buffer consumed by the /message API
func (kc *KafkaClient) saveMessage(msg *sarama.ConsumerMessage) {
	kc.ml.Lock()
	defer kc.ml.Unlock()
	if len(kc.receivedMessages) >= 10 {
		kc.receivedMessages = kc.receivedMessages[1:]
	}
	kc.receivedMessages = append(kc.receivedMessages, Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}

// Setup the Kafka client for producing and consumer messages.
// Use the specified configuration environment variables.
func newKafkaClient(config *AppConfig) *KafkaClient {
	tlsConfig := config.createTLSConfig()
	brokerAddrs := config.brokerAddresses()

	// verify broker certs
	for _, b := range brokerAddrs {
		ok, err := verifyServerCert(tlsConfig, config.Kafka.TrustedCert, b)
		if err != nil {
			log.Fatal("Get Server Cert Error: ", err)
		}

		if !ok {
			log.Fatalf("Broker %s has invalid certificate!", b)
		}
	}
	log.Println("All broker server certificates are valid!")

	return &KafkaClient{
		consumer: config.createKafkaConsumer(brokerAddrs, tlsConfig),
		producer: config.createKafkaProducer(brokerAddrs, tlsConfig),
	}
}

func verifyServerCert(tc *tls.Config, caCert string, url string) (bool, error) {
	// Create connection to server
	conn, err := tls.Dial("tcp", url, tc)
	if err != nil {
		return false, err
	}

	// Pull servers cert
	serverCert := conn.ConnectionState().PeerCertificates[0]

	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(caCert))
	if !ok {
		return false, errors.New("Unable to parse Trusted Cert")
	}

	// Verify Server Cert
	opts := x509.VerifyOptions{Roots: roots}
	if _, err := serverCert.Verify(opts); err != nil {
		log.Println("Unable to verify Server Cert")
		return false, err
	}

	return true, nil
}

// Create the TLS context, using the key and certificates provided.
func (ac *AppConfig) createTLSConfig() *tls.Config {
	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(ac.Kafka.TrustedCert))
	if !ok {
		log.Println("Unable to parse Root Cert:", ac.Kafka.TrustedCert)
	}

	// Setup certs for Sarama
	cert, err := tls.X509KeyPair([]byte(ac.Kafka.ClientCert), []byte(ac.Kafka.ClientCertKey))
	if err != nil {
		log.Fatal(err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            roots,
	}

	tlsConfig.BuildNameToCertificate()
	return tlsConfig
}

// Connect a consumer. Consumers in Kafka have a "group" id, which
// denotes how consumers balance work. Each group coordinates
// which partitions to process between its nodes.
// For the demo app, there's only one group, but a production app
// could use separate groups for e.g. processing events and archiving
// raw events to S3 for longer term storage
func (ac *AppConfig) createKafkaConsumer(brokers []string, tc *tls.Config) *cluster.Consumer {
	config := cluster.NewConfig()

	config.Net.TLS.Config = tc
	config.Net.TLS.Enable = true
	config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	config.ClientID = ac.Kafka.ConsumerGroup
	config.Consumer.Return.Errors = true

	topic := ac.topic()

	log.Printf("Consuming topic %s on brokers: %s", topic, brokers)

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := cluster.NewConsumer(brokers, ac.group(), []string{topic}, config)
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

// Create the Kafka asynchronous producer
func (ac *AppConfig) createKafkaProducer(brokers []string, tc *tls.Config) sarama.AsyncProducer {
	config := sarama.NewConfig()

	config.Net.TLS.Config = tc
	config.Net.TLS.Enable = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Default is WaitForLocal
	config.ClientID = ac.Kafka.ConsumerGroup

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

// Extract the host:port pairs from the Kafka URL(s)
func (ac *AppConfig) brokerAddresses() []string {
	urls := strings.Split(ac.Kafka.URL, ",")
	addrs := make([]string, len(urls))
	for i, v := range urls {
		u, err := url.Parse(v)
		if err != nil {
			log.Fatal(err)
		}
		addrs[i] = u.Host
	}
	return addrs
}

// Prepends prefix to topic if provided
func (ac *AppConfig) topic() string {
	topic := ac.Kafka.Topic

	if ac.Kafka.Prefix != "" {
		topic = strings.Join([]string{ac.Kafka.Prefix, topic}, "")
	}

	return topic
}

// Prepend prefix to consumer group if provided
func (ac *AppConfig) group() string {
	group := ac.Kafka.ConsumerGroup

	if ac.Kafka.Prefix != "" {
		group = strings.Join([]string{ac.Kafka.Prefix, group}, "")
	}

	return group
}
