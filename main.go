package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/heroku/heroku-kafka-demo-go/internal/config"
	"github.com/heroku/heroku-kafka-demo-go/internal/transport"
	"github.com/joeshaw/envdecode"
)

var topic string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	appconfig := config.AppConfig{}
	envdecode.MustDecode(&appconfig)

	client, err := transport.NewKafkaClient(&appconfig)
	if err != nil {
		log.Fatal(err)
	}

	topic = appconfig.Topic()
	buffer := transport.MessageBuffer{}
	handler := transport.NewMessageHandler(&buffer)

	go client.ConsumeMessages(ctx, []string{topic}, handler)

	router := gin.New()
	router.Use(gin.Logger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	router.GET("/", indexGET)
	router.GET("/messages", buffer.GetMessages)
	router.POST("/messages/:topic", client.PostMessage)

	err = router.Run(":" + appconfig.Web.Port)
	if err != nil {
		client.Producer.Close()
		client.Consumer.Close()
		cancel()
		log.Fatal(err)
	}

	client.Producer.Close()
	client.Consumer.Close()
	cancel()
}

func indexGET(c *gin.Context) {
	h := gin.H{
		"baseurl": "https://" + c.Request.Host,
		"topic":   topic,
	}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}
