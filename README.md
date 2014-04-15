avro-1.7.4-schema-repo
======================
## Description
  Avro-1.7.4-schema-repo is from patch AVRO-1124 applied to avro-1.7.4
  
## Compilation

  mvn clean package
  
## Run

  java -jar avro-repo-bundle-1.7.4-withdeps.jar config.properties
  
## Configuration

  TODO
  
## How it works

  - A Kafka producer send an avro schema to the avro schema repository server. 
  - The server looks if the schema received is new or was previously registered.
  - If it's new, stores it and returns an unique ID ( incremental integer).
  - If it was previously registered returns the previously assigned ID.
  - The Kafka producer writes in the topic the Avro message with this ID instead of the schema.
    
  - A kafka consumer (camus) reads from Kafka Topic.
  - Camus asks the schema repository server for the schema associated to the ID of the message readed.
  - Substitute the Header with the ID for the original schema of the message.
  - Writes in HDFS
