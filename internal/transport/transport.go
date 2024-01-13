package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/heroku/heroku-kafka-demo-go/internal/config"
)

// Message represents a Kafka message
type Message struct {
	Metadata  MessageMetadata `json:"metadata"`
	Value     string          `json:"value"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
}

// MessageMetadata represents metadata about a Kafka message
type MessageMetadata struct {
	ReceivedAt time.Time `json:"receivedAt"`
}

// MessageBuffer is a buffer of Kafka messages
// Concurrent access to the buffer is protected via a RWMutex
type MessageBuffer struct {
	receivedMessages []Message
	MaxSize          int

	ml sync.RWMutex
}

// GetMessages returns the messages in the buffer
// returning it in JSON format
func (mb *MessageBuffer) GetMessages(c *gin.Context) {
	mb.ml.RLock()
	defer mb.ml.RUnlock()

	c.JSON(http.StatusOK, mb.receivedMessages)
}

// SaveMessage saves a message to the buffer
func (mb *MessageBuffer) SaveMessage(msg Message) {
	mb.ml.Lock()
	defer mb.ml.Unlock()

	if len(mb.receivedMessages) >= mb.MaxSize {
		mb.receivedMessages = mb.receivedMessages[1:]
	}

	mb.receivedMessages = append(mb.receivedMessages, msg)
}

// MessageHandler is a Sarama consumer group handler
type MessageHandler struct {
	Ready  chan bool
	buffer *MessageBuffer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *MessageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.Ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *MessageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// saveMessage saves a consumer message to the buffer
func (c *MessageHandler) saveMessage(msg *sarama.ConsumerMessage) {
	c.buffer.SaveMessage(Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *MessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				slog.Error("message channel was closed")
				return nil
			}

			c.saveMessage(msg)
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

// NewMessageHandler creates a new MessageHandler
func NewMessageHandler(buffer *MessageBuffer) *MessageHandler {
	return &MessageHandler{
		Ready:  make(chan bool),
		buffer: buffer,
	}
}

// CreateKafkaProducer creates a new Sarama SyncProducer
func CreateKafkaProducer(ac *config.AppConfig) (sarama.SyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 10
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Compression = sarama.CompressionZSTD
	kafkaConfig.ClientID = "heroku-kafka-demo-go/producer"

	tlsConfig := ac.CreateTLSConfig()
	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Net.TLS.Config = tlsConfig

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(ac.BrokerAddresses(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// CreateKafkaConsumer creates a new Sarama ConsumerGroup
func CreateKafkaConsumer(ac *config.AppConfig) (sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()
	tlsConfig := ac.CreateTLSConfig()
	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Net.TLS.Config = tlsConfig

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	kafkaConfig.ClientID = "heroku-kafka-demo-go/consumer"
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
	kafkaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumerGroup(ac.BrokerAddresses(), ac.Group(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// KafkaClient is a wrapper around a Sarama SyncProducer and ConsumerGroup
type KafkaClient struct {
	Producer sarama.SyncProducer
	Consumer sarama.ConsumerGroup
}

// NewKafkaClient creates a new KafkaClient
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

// Close closes the KafkaClient
func (kc *KafkaClient) Close() {
	kc.Producer.Close()
	kc.Consumer.Close()
}

// SendMessage sends a message to Kafka
func (kc *KafkaClient) SendMessage(topic, key string, message []byte) error {
	_, _, err := kc.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(message),
	})

	return err
}

// PostMessage is a handler for POST /messages/:topic
func (kc *KafkaClient) PostMessage(c *gin.Context) {
	message, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = kc.SendMessage(c.Param("topic"), c.Request.RemoteAddr, message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
}

// ConsumeMessages consumes messages from Kafka
func (kc *KafkaClient) ConsumeMessages(ctx context.Context, topics []string, handler *MessageHandler) {
	for {
		if err := kc.Consumer.Consume(ctx, topics, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			slog.Error(
				"error consuming",
				"err", err,
			)
		}

		if ctx.Err() != nil {
			return
		}

		handler.Ready = make(chan bool)
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
		slog.Error("Unable to verify Server Cert")
		return false, err
	}

	return true, nil
}
