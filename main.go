package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/heroku/heroku-kafka-demo-go/internal/config"
	"github.com/heroku/heroku-kafka-demo-go/internal/transport"
)

const (
	// MaxBufferSize is the maximum number of messages to keep in the buffer
	MaxBufferSize = 10
)

type IndexHandler struct {
	Topic string
}

func (i *IndexHandler) GetIndex(c *gin.Context) {
	h := gin.H{
		"baseurl": "https://" + c.Request.Host,
		"topic":   i.Topic,
	}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}

func slogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		slog.Info(
			"request",
			"method", c.Request.Method,
			"ip", c.ClientIP(),
			"ua", c.Request.UserAgent(),
			"path", path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}

func main() {
	if os.Getenv("KAFKA_DEBUG") != "" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	ctx, cancel := context.WithCancel(context.Background())

	appconfig, err := config.NewAppConfig()
	if err != nil {
		slog.Error(
			"error loading config",
			"at", "main",
			"error", err,
		)
	}

	slog.Info(
		"starting up",
		"at", "main",
		"topic", appconfig.Topic(),
		"port", appconfig.Web.Port,
		"consumer_group", appconfig.Group(),
		"broker_addresses", appconfig.BrokerAddresses(),
		"prefix:", appconfig.Kafka.Prefix,
	)

	client, err := transport.NewKafkaClient(appconfig)
	if err != nil {
		log.Fatal(err)
	}

	topic := appconfig.Topic()
	buffer := transport.MessageBuffer{
		MaxSize: MaxBufferSize,
	}
	consumerHandler := transport.NewMessageHandler(&buffer)

	go client.ConsumeMessages(ctx, []string{topic}, consumerHandler)

	slog.Info(
		"waiting for consumer to be ready",
		"at", "main",
		"topic", topic,
	)

	start := time.Now()

	<-consumerHandler.Ready

	slog.Info(
		"consumer is ready",
		"at", "main",
		"topic", topic,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	router := gin.New()
	router.Use(slogger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	ih := &IndexHandler{Topic: topic}

	router.GET("/", ih.GetIndex)
	router.GET("/messages", buffer.GetMessages)
	router.POST("/messages/:topic", client.PostMessage)
	router.POST("/async-messages/:topic", client.PostAsyncMessage)

	err = router.Run(":" + appconfig.Web.Port)
	if err != nil {
		client.Close()
		cancel()
		log.Fatal(err)
	}

	client.Close()
	cancel()
}
