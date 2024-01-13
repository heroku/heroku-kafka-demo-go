package config

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/url"
	"strings"
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

func (ac *AppConfig) CreateTLSConfig() *tls.Config {
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
		InsecureSkipVerify: true, //#nosec G402
		RootCAs:            roots,
	}

	return tlsConfig
}

func (ac *AppConfig) BrokerAddresses() []string {
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

func (ac *AppConfig) Topic() string {
	topic := ac.Kafka.Topic

	if ac.Kafka.Prefix != "" {
		topic = ac.Kafka.Prefix + topic
	}

	return topic
}

func (ac *AppConfig) GroupID() string {
	group := ac.Kafka.ConsumerGroup

	if ac.Kafka.Prefix != "" {
		group = ac.Kafka.Prefix + group
	}

	return group
}
