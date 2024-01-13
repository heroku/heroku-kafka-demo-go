package config

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/url"
	"strings"

	"github.com/joeshaw/envdecode"
)

// KafkaConfig is the configuration for Kafka
type KafkaConfig struct {
	URL           string `env:"KAFKA_URL,required"`
	TrustedCert   string `env:"KAFKA_TRUSTED_CERT,required"`
	ClientCertKey string `env:"KAFKA_CLIENT_CERT_KEY,required"`
	ClientCert    string `env:"KAFKA_CLIENT_CERT,required"`
	Prefix        string `env:"KAFKA_PREFIX"`
	Topic         string `env:"KAFKA_TOPIC,default=messages"`
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP,default=heroku-kafka-demo-go"`
}

// WebConfig is the configuration for the web server
type WebConfig struct {
	Port string `env:"PORT,required"`
}

// AppConfig is the configuration for the application
type AppConfig struct {
	Kafka KafkaConfig
	Web   WebConfig
}

// NewAppConfig returns a new AppConfig
func NewAppConfig() (*AppConfig, error) {
	cfg := &AppConfig{}

	err := envdecode.Decode(cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// CreateTLSConfig creates a TLS config for the Kafka client
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

// BrokerAddresses returns a list of Kafka broker addresses
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

// Topic returns the Kafka topic to use
func (ac *AppConfig) Topic() string {
	topic := ac.Kafka.Topic

	if ac.Kafka.Prefix != "" {
		topic = ac.Kafka.Prefix + topic
	}

	return topic
}

// GroupID returns the Kafka consumer group ID to use
func (ac *AppConfig) Group() string {
	group := ac.Kafka.ConsumerGroup

	if ac.Kafka.Prefix != "" {
		group = ac.Kafka.Prefix + group
	}

	return group
}
