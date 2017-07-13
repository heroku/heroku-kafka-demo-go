package main

import (
	"os"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
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
	config   *AppConfig

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
	router.GET("/cert-check", client.certCheckGET)
	router.GET("/messages", client.messagesGET)
	router.POST("/messages", client.messagesPOST)

	router.Run(":" + appconfig.Web.Port)
}

func indexGET(c *gin.Context) {
	h := gin.H{"baseurl": "https://" + c.Request.Host}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}

type certCheckResult struct {
	Ok bool `json:"ok?"`
	Broker string `json:"broker"`
	TrustedCertType string `json:"trusted_cert_type"`
	ClientCertType string `json:"client_cert_type"`
	Err string `json:"err"`
	Topic string `topic:"err"`
}

// this memory re-verifies broker certs
func (kc *KafkaClient) certCheckGET(c *gin.Context) {
	checks := []certCheckResult{}
	for trustedCertType, trustedCertPrefix := range map[string]string { "trusted_cert_both": "", "trusted_cert_legacy": "_LEGACY", "trusted_cert_modern": "_MODERN" } {
		for clientCertType, clientCertPrefix := range map[string]string { "client_cert_both": "", "client_cert_legacy": "_LEGACY", "client_cert_modern": "_MODERN" } {
			trustedCertConfigVar := "KAFKA" + trustedCertPrefix + "_TRUSTED_CERT"
			trustedCert := os.Getenv(trustedCertConfigVar)
			if trustedCert == "" {
				checks = append(checks, certCheckResult{
					Ok: false,
					TrustedCertType: trustedCertType,
					ClientCertType: clientCertType,
					Err: fmt.Sprintf("could not find a trusted cert in %s", trustedCertConfigVar),
				})
				continue
			}

			clientCertConfigVar := "KAFKA" + clientCertPrefix + "_CLIENT_CERT"
			clientCert := os.Getenv(clientCertConfigVar)
			if trustedCert == "" {
				checks = append(checks, certCheckResult{
					Ok: false,
					TrustedCertType: trustedCertType,
					ClientCertType: clientCertType,
					Err: fmt.Sprintf("could not find a client cert in %s", clientCertConfigVar),
				})
			}

			clientCertKeyConfigVar := "KAFKA" + clientCertPrefix + "_CLIENT_CERT_KEY"
			clientCertKey := os.Getenv(clientCertKeyConfigVar)
			if trustedCert == "" {
				checks = append(checks, certCheckResult{
					Ok: false,
					TrustedCertType: trustedCertType,
					ClientCertType: clientCertType,
					Err: fmt.Sprintf("could not find a client cert key in %s", clientCertKeyConfigVar),
				})
			}

			brokerAddrs := kc.config.brokerAddresses()

			tlsConfig, err := createTLSConfig(
				[]byte(trustedCert),
				[]byte(clientCert),
				[]byte(clientCertKey),
			)
			if err != nil {
				checks = append(checks, certCheckResult{
					Ok: false,
					TrustedCertType: trustedCertType,
					ClientCertType: clientCertType,
					Err: fmt.Sprintf("tls config load error %s", err.Error()),
				})
				continue
			}

			for _, b := range brokerAddrs {
				verifyErrs := verifyBrokers([]string { b }, tlsConfig, trustedCert)
				if len(verifyErrs) != 0 {
					for _, err := range verifyErrs {
						checks = append(checks, certCheckResult{
							Ok: false,
							Broker: b,
							TrustedCertType: trustedCertType,
							ClientCertType: clientCertType,
							Err: fmt.Sprintf("verify cert error %s", err.Error()),
						})
					}
					continue
				}
				broker, err := dial(b, tlsConfig)
				if err != nil {
					checks = append(checks, certCheckResult{
						Ok: false,
						Broker: b,
						TrustedCertType: trustedCertType,
						ClientCertType: clientCertType,
						Err: fmt.Sprintf("connect to broker error %s", err.Error()),
					})
					continue
				}
				req := &sarama.MetadataRequest{Topics: nil}
				response, err := broker.GetMetadata(req)
				if err != nil {
					checks = append(checks, certCheckResult{
						Ok: false,
						Broker: b,
						TrustedCertType: trustedCertType,
						ClientCertType: clientCertType,
						Err: fmt.Sprintf("load metadata error %s", err.Error()),
					})
					continue
				}
				for _, topic := range response.Topics {
					checks = append(checks, certCheckResult{
						Ok: true,
						Broker: b,
						TrustedCertType: trustedCertType,
						ClientCertType: clientCertType,
						Err: "",
						Topic: topic.Name,
					})
				}
			}
		}
	}
	c.JSON(http.StatusOK, checks)
}

func dial(addr string, conf *tls.Config) (*sarama.Broker, error) {
	broker := sarama.NewBroker(addr)
	config := sarama.NewConfig()

	config.Net.TLS.Config = conf
	config.Net.TLS.Enable = true

	if err := broker.Open(config); err != nil {
		return nil, err
	}

	if _, err := broker.Connected(); err != nil {
		return nil, err
	}

	return broker, nil
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
	tlsConfig, err := config.createTLSConfig()
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to make tls config during startup %v", err))
	}

	brokerAddrs := config.brokerAddresses()

	errs := verifyBrokers(brokerAddrs, tlsConfig, config.Kafka.TrustedCert)
	if len(errs) != 0 {
		log.Fatal(fmt.Sprintf("%v", errs))
	}

	return &KafkaClient{
		config:   config,
		consumer: config.createKafkaConsumer(brokerAddrs, tlsConfig),
		producer: config.createKafkaProducer(brokerAddrs, tlsConfig),
	}
}

func verifyBrokers(brokerAddrs []string, tlsConfig *tls.Config, caCert string) []error {
	errs := []error{}
	// verify broker certs
	for _, b := range brokerAddrs {
		ok, err := verifyServerCert(tlsConfig, caCert, b)
		if err != nil {
			errs = append(errs, fmt.Errorf("Get Server Cert Error: %s for broker %s", err, b))
		}

		if !ok {
			errs = append(errs, fmt.Errorf("Get Server Cert Error: %s for broker %s", err, b))
		}
	}
	return errs
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
func (ac *AppConfig) createTLSConfig() (*tls.Config, error) {
	return createTLSConfig(
		[]byte(ac.Kafka.TrustedCert),
		[]byte(ac.Kafka.ClientCert),
		[]byte(ac.Kafka.ClientCertKey),
	)
}

func createTLSConfig(caCert []byte, clientCert []byte, clientCertKey []byte) (*tls.Config, error) {
	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(caCert))
	if !ok {
		return nil, fmt.Errorf("Unable to parse Root Cert: %s", caCert)
	}

	// Setup certs for Sarama
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientCertKey))
	if err != nil {
		return nil, fmt.Errorf("Unable to parse client cert: %s", err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            roots,
	}

	tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
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
