# Kafka with Java 
### This project is to demonstrate the programmatic implementaion of kafka using kafka-client library aloong with Java

# Installation guide
* Java 11 or later is required 
* Execute command ```mvn -version``` to check the installed maven version first

# Run the program
* Program contains ```Environment variables``` hence runtime arguments needs to be provided
* To launch ```producer``` execute ```mvn compile exec:java -Dexec.args="producer"```
* To launch ```producer``` execute ```mvn compile exec:java -Dexec.args="consumer"```

# Run kafka docker instace
```
docker run -it --name kafka-zkless -p 9092:9092
-e LOG_DIR=/tmp/logs quay.io/strimzi/kafka:latest-kafka-2.8.1-amd64 /bin/sh
-c 'export CLUSTER_ID=$(bin/kafka-storage.sh random-uuid) && bin/kafka-storage.sh format
-t $CLUSTER_ID
-c config/kraft/server.properties && bin/kafka-server-start.sh config/kraft/server.properties'
```
