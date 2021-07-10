package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	heroku "github.com/deadmanssnitch/sarama-heroku"
	"github.com/gin-gonic/gin"
	"github.com/joeshaw/envdecode"
)

type AppConfig struct {
	Kafka struct {
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
	return &KafkaClient{
		consumer: config.createKafkaConsumer(),
		producer: config.createKafkaProducer(),
	}
}

// Connect a consumer. Consumers in Kafka have a "group" id, which
// denotes how consumers balance work. Each group coordinates
// which partitions to process between its nodes.
// For the demo app, there's only one group, but a production app
// could use separate groups for e.g. processing events and archiving
// raw events to S3 for longer term storage
func (ac *AppConfig) createKafkaConsumer() *cluster.Consumer {
	config := cluster.NewConfig()
	config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	config.ClientID = ac.Kafka.ConsumerGroup
	config.Consumer.Return.Errors = true

	topic := ac.topic()
	brokers, err := heroku.Brokers()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Consuming topic %s on brokers: %s", topic, brokers)

	consumer, err := heroku.NewClusterConsumer(ac.group(), []string{topic}, config)
	if err != nil {
		log.Fatal(err)
	}

	return consumer
}

// Create the Kafka asynchronous producer
func (ac *AppConfig) createKafkaProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Default is WaitForLocal
	config.ClientID = ac.Kafka.ConsumerGroup

	producer, err := heroku.NewAsyncProducer(config)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

// Prepends prefix to topic if provided
func (ac *AppConfig) topic() string {
	return heroku.AppendPrefixTo(ac.Kafka.Topic)
}

// Prepend prefix to consumer group if provided
func (ac *AppConfig) group() string {
	return heroku.AppendPrefixTo(ac.Kafka.ConsumerGroup)
}
