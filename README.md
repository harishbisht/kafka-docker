# Kafka Producer and Consumer using docker and docker-compose example

- Running the kafka and zookeeper on the docker container
- Using the protobuf to serialize the messages
- Created a producer and consumer in python language

### Installation

Install docker and docker-compose from official docker website and run the below commands

#### It will pull the official kafka and zookeeper docker images and run it inside the kafka-network

```sh
$ cd kafka_and_zookeeper
$ docker-compose up -d
```
#### It will start producing the messages and push to the kafka
```sh
$ cd producer
$ ./build.sh
$ ./start.sh
```

#### It will start receiving the messages from kafka and process the messages
```sh
$ cd consumer
$ ./build.sh
$ ./start.sh
```

Thanks for reading and please contribute (: