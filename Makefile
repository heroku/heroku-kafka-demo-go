TARGET := bin/heroku-kafka-demo-go

build:
	go build -o $(TARGET)
.PHONY: build

lint:
	golangci-lint run ./...
.PHONY: lint
