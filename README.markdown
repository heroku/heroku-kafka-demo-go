# heroku-kafka-demo-go

A simple heroku app that demonstrates using Kafka in Go.
This demo app accepts HTTP POST requests and writes them to a topic, and has a simple page that shows the last 10 messages produced to that topic.

You'll need to [provision](#provisioning) the app.

## Provisioning

Install the kafka cli plugin:

```
$ heroku plugins:install heroku-kafka
```

Create a heroku app with Kafka attached:

```
$ heroku create
$ heroku addons:create heroku-kafka:beta-dev
$ heroku kafka:wait
```

Create the sample topic, by default the topic will have 32 partitions:

```
$ heroku kafka:create messages --partitions 1
```

Deploy to Heroku and open the app:

```
$ git push heroku master
$ heroku open
```
