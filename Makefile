TARGET := bin/heroku-kafka-demo-go

build:
	go build -race -o $(TARGET)
.PHONY: build

lint:
	golangci-lint run ./...
.PHONY: lint

test:
	GIN_MODE=release go test -race -v ./...
.PHONY: test
