This is a Kafka consumer that reads mock Clickstream data for an imaginary e-commerce site from an Apache Kafka topic. 
It utilizes the Schema Registry and reads Avro encoded events. Consequently, the location of the Schema Registry needs to 
be specified in a **consumer.properties** file. The number of consumers to be run in separate threads can be specified and they will be run in the same consumer group.

This consumer works with the constructs in [MirrorMaker2](https://cwiki.apache.org/confluence/display/KAFKA/KIP-382%3A+MirrorMaker+2.0).
MirrorMaker v2 (MM2), which ships as part of Apache Kafka in version 2.4.0 and above, detects and 
replicates topics, topic partitions, topic configurations and topic ACLs to the destination cluster that matches a regex topic pattern. 
Further, it checks for new topics that matches the topic pattern or changes to configurations and ACLs at regular configurable intervals. 
The topic pattern can also be dynamically changed by changing the configuration of the MirrorSourceConnector. 
Therefore MM2 can be used to migrate topics and topic data to the destination cluster and keep them in sync.

When replicating messages in topics between clusters, the offsets in topic partitions could be different 
due to producer retries or more likely due to the fact that the retention period in the source topic could've passed 
and messages in the source topic already deleted when replication starts. Even if the the __consumer_offsets topic is replicated, 
the consumers, on failover, might not find the offsets at the destination.

MM2 provides a facility that keeps source and destination offsets in sync. The MM2 MirrorCheckpointConnector periodically 
emits checkpoints in the destination cluster, containing offsets for each consumer group in the source cluster. 
The connector periodically queries the source cluster for all committed offsets from all consumer groups, filters for 
topics being replicated, and emits a message to a topic like \<source-cluster-alias\>.checkpoints.internal in the destination cluster. 
These offsets can then be queried and retrieved by using provided classes **RemoteClusterUtils** or **MirrorClient**. This 
consumer accepts a parameter (-flo) which indicates if the consumer has failed over. In which case, it utilizes the RemoteClusterUtils class to 
get the translated offsets at the destination, and seeks to it. Since the MirrorCheckpointConnector emits checkpoints periodically, 
there could be additional offsets read by the consumer at the source that were not checkpointed when the consumer failed over. To 
account for that in order to minimize duplicates, this consumer writes the last offset processed for each topic partition to a file when stopped. On failover, 
it reads the file and calculates the difference in offsets between the checkpointed offsets and the last offset read at the source and 
skips the equivalent number of messages at the destination after translation and resumes reading.

In addition, if the consumer offsets are being synced between the MM2 checkpointed offsets and the __consumer_offsets 
at the destination in the background, this consumer can also be used to simply start reading from the last committed offset 
at the destination.

This consumer can be used to read other types of events by modifying the RunConsumer class.

## Install

### Clone the repository and install the jar file.  

    mvn clean package -f pom.xml
    
   ### The consumer accepts a number of command line parameters:
   
   * ***--help (or -h): help to get list of parameters***
   * ***--topic (or -t)***: Apache Kafka topic to send events to. Default ***ExampleTopic***.
   * ***--propertiesFilePath (or -pfp)***: Location of the consumer properties file which contains information about the Apache Kafka bootstrap brokers and the location of the Confluent Schema Registry. Default ***/tmp/kafka/producer.properties***.
   * ***--numThreads (or -nt)***: Number of threads to run in parallel. Default ***2***.
   * ***--runFor (or -rf)***: Number of seconds to run the producer for.
   * ***--sslEnable (or -ssl)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption.
   * ***--mTLSEnable (or -mtls)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption and TLS mutual authentication. If this parameter is specified, TLS is also enabled. This reads the specified properties file for **SSL_TRUSTSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_PASSWORD_CONFIG** and **SSL_KEY_PASSWORD_CONFIG**. Those properties need to be specified in the properties file.
   * ***--failover (or -flo)***: Whether the consumer has failover to the destination cluster.
   * ***--sourceRewind (or -srr)***: Whether to rewind the consumer to start from the beginning of the retention period.
   * ***--sourceCluster (or -src)***: The alias of the source cluster specified in the MM2 configuration.
   * ***--destCluster (or -dst)***: The alias of the destination cluster specified in the MM2 configuration.
   * ***--destCluster (or -dst)***: The alias of the destination cluster specified in the MM2 configuration.
   * **-rpc (or --replicationPolicyClass)**: The class name of the replication policy to use. Works with a custom MM2 replication policy.
   * **-rps (or --replicationPolicySeparator)**: The separator to use with the DefaultReplicationPolicy between the source cluster alias and the topic name.

   
   ### Example usage:
   
   #### At the source and the destination with a background process to sync MM2 checkpointed offsets to the __consumer_offsets internal topic at the destination.
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -mtls -src msksource
   ```

#### At the destination using the consumer to query and snap to MM2 checkpointed offsets.
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -mtls -flo -src msksource -dst mskdest
   ```