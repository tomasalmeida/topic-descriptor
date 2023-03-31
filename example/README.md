# Test

./startCluster.sh

kafka-topics --bootstrap-server localhost:29092 --create --topic topic-test

kafka-console-producer --broker-list localhost:29092 --topic topic-test --property parse.key=true --property key.separator=,

kafka-console-consumer --bootstrap-server localhost:29092 --from-beginning --property print.key=true  --topic topic-test

java -jar target/topic-descriptor-0.1.0-SNAPSHOT.jar example/config.properties topic-test
