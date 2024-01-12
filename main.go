package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	// cluster "github.com/bsm/sarama-cluster"
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
	consumer sarama.ConsumerGroup

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

type Consumer struct {
	ready chan bool

	ml               sync.RWMutex
	receivedMessages []Message
}

// Save consumed message in the buffer consumed by the /message API
func (consumer *Consumer) saveMessage(msg *sarama.ConsumerMessage) {
	consumer.ml.Lock()
	defer consumer.ml.Unlock()
	if len(consumer.receivedMessages) >= 10 {
		consumer.receivedMessages = consumer.receivedMessages[1:]
	}
	consumer.receivedMessages = append(consumer.receivedMessages, Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}

// This endpoint accesses in memory state gathered
// by the consumer, which holds the last 10 messages received
func (consumer *Consumer) messagesGET(c *gin.Context) {
	consumer.ml.RLock()
	defer consumer.ml.RUnlock()
	c.JSON(http.StatusOK, consumer.receivedMessages)
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			consumer.saveMessage(message)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

var topic string

func main() {
	ctx := context.Background()
	appconfig := AppConfig{}
	envdecode.MustDecode(&appconfig)

	client := newKafkaClient(&appconfig)
	client.receivedMessages = make([]Message, 0)
	consumer := Consumer{}

	topic = appconfig.topic()

	go client.consumeMessages(ctx, []string{topic}, &consumer)
	defer client.producer.Close()
	defer client.consumer.Close()

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	router.GET("/", indexGET)
	router.GET("/messages", consumer.messagesGET)
	router.POST("/messages", client.messagesPOST)

	router.Run(":" + appconfig.Web.Port)
}

func indexGET(c *gin.Context) {
	h := gin.H{"baseurl": "https://" + c.Request.Host}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}

// A sample producer endpoint.
// It receives messages as http bodies on /messages,
// and posts them directly to a Kafka topic.
func (kc *KafkaClient) messagesPOST(c *gin.Context) {
	message, err := io.ReadAll(c.Request.Body)
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
func (kc *KafkaClient) consumeMessages(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) {
	for {
		err := kc.consumer.Consume(ctx, topics, handler)
		if err != nil {
			log.Print(err)
		}
	}
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
		return false, errors.New("unable to parse trusted cert")
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
func (ac *AppConfig) createKafkaConsumer(brokers []string, tc *tls.Config) sarama.ConsumerGroup {
	config := sarama.NewConfig()

	config.Net.TLS.Config = tc
	config.Net.TLS.Enable = true
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.ClientID = ac.Kafka.ConsumerGroup
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	// log.Printf("Consuming topic %s on brokers: %s", topic, brokers)

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumerGroup(brokers, ac.group(), config)
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
