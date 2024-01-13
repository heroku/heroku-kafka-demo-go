package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/heroku/heroku-kafka-demo-go/internal/config"
)

const (
	// The number of messages to keep in the buffer
	// before dropping the oldest message.
	MaxBufferSize = 10
)

type Message struct {
	Metadata  MessageMetadata `json:"metadata"`
	Value     string          `json:"value"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
}

type MessageMetadata struct {
	ReceivedAt time.Time `json:"received_at"`
}

type MessageBuffer struct {
	receivedMessages []Message

	ml sync.RWMutex
}

func (mb *MessageBuffer) GetMessages(c *gin.Context) {
	mb.ml.RLock()
	defer mb.ml.RUnlock()

	c.JSON(http.StatusOK, mb.receivedMessages)
}

type MessageHandler struct {
	ready  chan bool
	buffer *MessageBuffer
}

func (c *MessageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)

	return nil
}

func (c *MessageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *MessageHandler) saveMessage(msg *sarama.ConsumerMessage) {
	c.buffer.ml.Lock()
	defer c.buffer.ml.Unlock()

	if len(c.buffer.receivedMessages) >= MaxBufferSize {
		c.buffer.receivedMessages = c.buffer.receivedMessages[1:]
	}

	c.buffer.receivedMessages = append(c.buffer.receivedMessages, Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}

func (c *MessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			c.saveMessage(msg)
			session.MarkMessage(msg, "")
		}
	}
}

func NewMessageHandler(buffer *MessageBuffer) *MessageHandler {
	return &MessageHandler{
		ready:  make(chan bool),
		buffer: buffer,
	}
}

func CreateKafkaProducer(ac *config.AppConfig) (sarama.AsyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true

	tlsConfig := ac.CreateTLSConfig()
	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Net.TLS.Config = tlsConfig

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(ac.BrokerAddresses(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func CreateKafkaConsumer(ac *config.AppConfig) (sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true

	tlsConfig := ac.CreateTLSConfig()
	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Net.TLS.Config = tlsConfig

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	kafkaConfig.ClientID = ac.GroupID()
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
	kafkaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumerGroup(ac.BrokerAddresses(), ac.GroupID(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

type KafkaClient struct {
	Producer sarama.AsyncProducer
	Consumer sarama.ConsumerGroup
}

func NewKafkaClient(ac *config.AppConfig) (*KafkaClient, error) {
	tlsConfig := ac.CreateTLSConfig()

	ok, err := verifyBrokers(tlsConfig, ac.Kafka.TrustedCert, ac.BrokerAddresses())
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New("unable to verify brokers")
	}

	producer, err := CreateKafkaProducer(ac)
	if err != nil {
		return nil, err
	}

	consumer, err := CreateKafkaConsumer(ac)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{
		Producer: producer,
		Consumer: consumer,
	}, nil
}

func (kc *KafkaClient) Close() {
	kc.Producer.AsyncClose()
	kc.Consumer.Close()
}

func (kc *KafkaClient) SendMessage(topic, key string, message []byte) {
	kc.Producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(message),
	}
}

func (kc *KafkaClient) PostMessage(c *gin.Context) {
	message, err := io.ReadAll(c.Request.Body)
	if err != nil {
		log.Fatal(err)
	}

	kc.SendMessage(c.Param("topic"), c.Request.RemoteAddr, message)
}

func (kc *KafkaClient) ConsumeMessages(ctx context.Context, topics []string, handler *MessageHandler) {
	for {
		if err := kc.Consumer.Consume(ctx, topics, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			log.Print(err)
		}

		if ctx.Err() != nil {
			return
		}

		handler.ready = make(chan bool)
	}
}

func verifyBrokers(tc *tls.Config, caCert string, urls []string) (bool, error) {
	for _, url := range urls {
		ok, err := verifyServerCert(tc, caCert, url)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func verifyServerCert(tc *tls.Config, caCert, url string) (bool, error) {
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
