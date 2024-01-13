package config

import (
	"testing"
)

func TestBrokerAddresses(t *testing.T) {
	kcfg := KafkaConfig{
		URL: "kafka+ssl://localhost:9091,kafka+ssl://localhost:9092,kafka+ssl://localhost:9093,kafka+ssl://localhost:9094,kafka+ssl://localhost:9095,kafka+ssl://localhost:9096,kafka+ssl://localhost:9097,kafka+ssl://localhost:9098",
	}
	cfg := &AppConfig{
		Kafka: kcfg,
	}

	addresses := cfg.BrokerAddresses()
	expectedLen := 8

	if len(addresses) != expectedLen {
		t.Errorf("Expected %d addresses, got %d", expectedLen, len(addresses))
	}

	if addresses[0] != "localhost:9091" {
		t.Errorf("Expected localhost:9091, got %s", addresses[0])
	}

	if addresses[1] != "localhost:9092" {
		t.Errorf("Expected localhost:9092, got %s", addresses[1])
	}

	if addresses[2] != "localhost:9093" {
		t.Errorf("Expected localhost:9093, got %s", addresses[2])
	}
}

func TestTopic(t *testing.T) {
	kcfg := KafkaConfig{
		Prefix: "foobar.",
		Topic:  "test-topic",
	}
	cfg := &AppConfig{
		Kafka: kcfg,
	}

	expected := "foobar.test-topic"

	if cfg.Topic() != expected {
		t.Errorf("Expected %s, got %s", expected, cfg.Topic())
	}
}

func TestGroup(t *testing.T) {
	kcfg := KafkaConfig{
		Prefix:        "foobar.",
		ConsumerGroup: "test-group",
	}
	cfg := &AppConfig{
		Kafka: kcfg,
	}

	expected := "foobar.test-group"

	if cfg.Group() != expected {
		t.Errorf("Expected %s, got %s", expected, cfg.Group())
	}
}
