# KAFKA
## _Implementing a Kafka cluster and Publishing/Consuming data to/from a Kafka topic_

In this project I will setup a Kafka Single broker cluster using docker. A producer application to publish data to kafka topic, And a consumer application to subscribe to this topic.

## Tech & Tools

- Using Confluent Kafka.
- Python used for producer and consumer application.
- Postrges DB to persist the consumer data. 
- Using live BTC Trades data in Producer app.

## Producer App

For the producer app I have used deribit publicaly available API to get Bitcoin live trades data.

Producer fetches the data from API and publish it to kafka topic. Additional processing can be done on data before publishing to topic.

> More information about this API can be found [here][data_link].

## Consumer App

Bitcoin live trades data is consumed by this application by subscribing to kafka topic in which producer is publishing data.

This can be processed and then stored or used as per requirement. For this project I am simply persisting the data into a Postgres DB table ``` btc_trades```.

## Running solution

We use Docker to run this solution on any machine. Run `docker compose up` , it will spin up all required containers. 3 main service are:

- ## Kafka
```
Accessed at -   http://localhost:9021
```

- ## Postrgres DB
DB can be accessed by any other tool also using below connection details.
```
Host : localhost
Database : mydb
Port : 5431
Uname : db_user
Password: pwd
```

[//]: #
   [data_link]: <https://docs.deribit.com/#public-get_last_trades_by_instrument>
