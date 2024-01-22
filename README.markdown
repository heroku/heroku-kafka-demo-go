# heroku-kafka-demo-go

A simple Heroku app that demonstrates using Apache Kafka on Heroku in Go.
This demo app accepts HTTP POST requests and writes them to a topic, and has a simple page that shows the last 10 messages produced to that topic.

You'll need to [provision](#provisioning) the app.

## Provisioning

Install the Kafka CLI plugin:

```
$ heroku plugins:install heroku-kafka
```

Clone the repo:

```
$ git clone git@github.com:heroku/heroku-kafka-demo-go.git
```

Create a heroku app with Apache Kafka on Heroku attached:

```
$ cd heroku-kafka-demo-go
$ heroku apps:create your-cool-app-name
$ heroku addons:create heroku-kafka:basic-0
```

Create the sample topic. By default, the topic will have 8 partitions:

```
$ heroku kafka:topics:create messages
```

Create the consumer group:

**Note:** This assumes that you are using a `basic-0` as specified above. This step is not necessary for `standard`, `private`, or `shield` Apache Kafka on Heroku plans.

```
$ heroku kafka:consumer-groups:create heroku-kafka-demo-go
```

Deploy to Heroku and open the app:

```
$ git push heroku main
$ heroku open
```
