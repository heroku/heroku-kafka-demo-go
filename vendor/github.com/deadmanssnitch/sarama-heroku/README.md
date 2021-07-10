# Sarama Heroku

[![GoDoc](https://godoc.org/github.com/deadmanssnitch/sarama-heroku?status.svg)](http://godoc.org/github.com/deadmanssnitch/sarama-heroku)

## Overview

sarama-heroku is a Go library that makes it easy to connect to [Heroku
Kafka](https://www.heroku.com/kafka).  We handle all the certificate management
and configuration so that you can start up your Kafka consumers and producers
with minimal effort.

## Installation

```console
go get -u github.com/deadmanssnitch/sarama-heroku
```

### Heroku

Make sure you have the Heroku CLI plugin installed:
```console
heroku plugins:install heroku-kafka
```

Next, you'll need to provision a new Kafka install or attach an existing one to
your app. To provision run:
```console
heroku addons:create heroku-kafka:basic-0 -a [app]
heroku kafka:wait -a [app]
```

## Consumers

Now you are ready to start using the library. From here there are two options.
You can either create your own configuration or it is possible to pass in a nil
config to create a cluster consumer based on the defaults configurations.

Create a cluster consumer using a custom config like the following:

```go
config := cluster.NewConfig()
config.Group.PartitionStrategy = cluster.StrategyRoundRobin
config.Group.Return.Notifications = true
config.ClientID = "app-name." + os.Getenv("DYNO")
config.Consumer.Return.Errors = true

consumer, err := heroku.NewClusterConsumer("group-id", []string{"topic"}, config)
```

:heavy_exclamation_mark: Multi-tenant plans require creating the consumer
groups before you can use them.
```console
heroku kafka:consumer-groups:create 'group-id' -a [app]
```

For an example on using sarama-cluster
[check the sarama-cluster repo](https://github.com/bsm/sarama-cluster) or
[see the documentation](https://godoc.org/github.com/bsm/sarama-cluster).

## Producers

There are multiple options for producers. Similar to consumers, you can specify
your own config or pass in a nil config for defaults. Furthermore, a producer
can be either Sync or Async. Read up on the differences
[here](https://godoc.org/github.com/Shopify/sarama).

Creating an async producer from a custom config:

```go
config := sarama.NewConfig()
config.Producer.Return.Errors = true
config.Producer.RequiredAcks = sarama.WaitForAll

producer, err := heroku.NewAsyncProducer(config)
```

:heavy_exclamation_mark: Multi-tenant plans require adding the KAFKA_PREFIX
when sending messages. You should use heroku.AppendPrefixTo("topic") to ensure
it's set.

```go
producer <- &sarama.ProducerMessage{
  Topic: heroku.AppendPrefixTo("events"),
  Key:   sarama.StringEncoder(key),
  Value: []byte("Message"),
}
```
For more information about how to set up a config see the
[sarama documentation](http://godoc.org/github.com/Shopify/sarama#Config).

## Environment

Sarama Heroku depends on the following environment variables that are set by Heroku Kafka

  - KAFKA\_CLIENT\_CERT
  - KAFKA\_CLIENT\_CERT\_KEY
  - KAFKA\_TRUSTED\_CERT
  - KAFKA\_PREFIX (only multi-tenant plans)
  - KAFKA\_URL

## Contributing
Thank you so much for your interest in contributing to this repository. We appreciate you and the work you're doing on this SO much.

**How often are we checking this repository for issues or PRs:**
If you post an issue or PR and do not hear back right away, do not worry! We aren't ignoring you. Expect to hear back within a couple of weeks. Typically, we check this repository once a month on the last Friday to review PRs, issues, and other maintenance duties.

**Submitting an Issue**

Before you contribute, please take a look at this [helpful article](https://opensource.guide/how-to-contribute/#how-to-submit-a-contribution) and follow the guidelines. It is important to supply enough context and information so that we can get to it as quickly as possible.

* Please include a screenshot
* Please include exact steps to replicate the bug or issue with as much information as possible
* Please include any "hunches" you may have about what the issue might be

**Submitting a PR:**

* Please [fork](https://help.github.com/articles/creating-a-pull-request-from-a-fork/) the respository, put your code change on a new branch and then create a pull request against master in the main repository.
* In the PR description, please include the following:
	- A link to the issue is there is one
	- A screenshot, gif, or detailed description of before and after functionality
	- Any applicable tests, if warranted
	- Instructions on how to QA if applicable

Thank you for contributing to open source software. Your work is helping us all make better software. Happy coding!
