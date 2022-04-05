## setting up the server

# Kafka/Zookeeper starter commands
```shell
    docker compose up -d
    docker container ls
    docker exec -it <IMAGEID> bash
    export PATH=$PATH:/opt/bitnami/kafka/bin
```
For topics
```shell
    kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic -partitions 3 --create
    kafka-topics.sh --bootstrap-server localhost:9092 --toic first_topic --describe
```
for producer
```shell
    kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic
```

for consumer
```shell
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic
```

### basic Terminologies

- Replication: having multiple copies of the data available across different servers
- Acks: do we wait for all reblicas to be in sync ? {0 , 1 , all}
- 