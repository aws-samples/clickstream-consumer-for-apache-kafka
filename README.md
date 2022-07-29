This is a Kafka consumer that reads mock Clickstream data for an imaginary e-commerce site from an Apache Kafka topic. 
It utilizes a Schema Registry and reads Avro encoded events. It supports both the AWS Glue Schema Registry and a 3rd party Schema Registry.

For the AWS Glue Schema Registry, the consumer accepts parameters to use a specific registry, pre-created schema name, 
schema description and a secondary deserialzer. If those parameters are not specified but using the AWS Glue Schema registry is specified, 
it uses the default schema registry. Some of the parameters may need to be specified if others are not. 
The AWS Glue Schema Registry provides open sourced serde libraries for serialization and deserialization which use the 
AWS default credentials chain (by default) for credentials and the region to construct an endpoint. One important thing 
to call out here is the use of the secondary deserializer with the AWSKafkaAvroDeserializer. The secondary deserializer 
allows the KafkaAvroDeserializer that integrates with the AWS Glue Schema Registry to use a specified secondary deserializer 
that points to a 3rd party Schema Registry. This enables the AWSKafkaAvroDeserializer to deserialize records that were 
not serialized using the AWS Glue Schema Registry. This is primarily useful when migrating from a 3rd party Schema Registry 
to the AWS Glue Schema Registry. With the secondary deserializer specified, the consumer can seamlessly deserialize records 
using both a 3rd party Schema Registry and the AWS Glue Schema Registry. In order to use it, the properties specific to 
the 3rd party deserializer need to be specified as well. For further information see the AWS Glue Schema Registry documentation.

For the 3rd party Schema Registry, the location of the Schema Registry needs to be specified in a **consumer.properties** file. 

The consumer supports running multiple consumers in multiple threads. The number of consumers to be run in separate threads 
can be specified and they will be run in the same consumer group.

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

This consumer supports TLS in-transit encryption, TLS mutual authentication and SASL/SCRAM authentication with Amazon MSK.
See the relevant parameters to enable them below.

This consumer can be used to read other types of events by modifying the RunConsumer class.

## Install

This consumer depends on another library to get secrets from AWS Secrets Manager for SAS/SCRAM authentication with Amazon MSK.
The library needs to be installed first before creating the jar file for the consumer.

### Clone and install the jar file

    git clone https://github.com/aws-samples/sasl-scram-secrets-manager-client-for-msk.git
    cd sasl-scram-secrets-manager-client-for-msk
    mvn clean install -f pom.xml
    
### If using this consumer with a custom replication policy with MirrorMaker 2.0

#### Clone and install the jar file for CustomMM2ReplicationPolicy

    git clone https://github.com/aws-samples/mirrormaker2-msk-migration.git
    cd mirrormaker2-msk-migration.git
    mvn clean install -f pom.xml

### Clone the repository and create the jar file.  

    mvn clean package -f pom.xml
    
   ### The consumer accepts a number of command line parameters:
   
   * ***--help (or -h): help to get list of parameters***
   * ***--topic (or -t)***: Apache Kafka topic to send events to. Default ***ExampleTopic***.
   * ***--propertiesFilePath (or -pfp)***: Location of the consumer properties file which contains information about the Apache Kafka bootstrap brokers and the location of the Confluent Schema Registry. Default ***/tmp/kafka/producer.properties***.
   * ***--numThreads (or -nt)***: Number of threads to run in parallel. Default 1.
   * ***--runFor (or -rf)***: Number of seconds to run the producer for.
   * ***--sslEnable (or -ssl)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption.
   * ***--mTLSEnable (or -mtls)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption and TLS mutual authentication. If this parameter is specified, TLS is also enabled. This reads the specified properties file for **SSL_TRUSTSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_PASSWORD_CONFIG** and **SSL_KEY_PASSWORD_CONFIG**. Those properties need to be specified in the properties file.
   * ***--saslscramEnable (or -sse)***: Enable SASL/SCRAM authentication between this application and Amazon MSK with in-transit encryption. If this parameter is specified, --saslscramUser (or -ssu) also needs to be specified. Also, this parameter cannot be specified with --mTLSEnable (or -mtls) or --sslEnable (or -ssl)
   * ***--iamEnable (or -iam)***: Enable AWS IAM authentication between this application and Amazon MSK with in-transit encryption. If this parameter is specified, this parameter cannot be specified with --mTLSEnable (or -mtls) or --sslEnable (or -ssl) or --saslscramEnable (or -sse)
   * ***--saslscramUser (or -ssu)***: The name of the SASL/SCRAM user stored in AWS Secrets Manager to be used for SASL/SCRAM authentication between this application and Amazon MSK. If this parameter is specified, --saslscramEnable (or -sse) also needs to be specified.
   * ***--region (or -reg)***: The region for AWS Secrets Manager storing secrets for SASL/SCRAM authentication with Amazon MSK. Default us-east-1.
   * ***--failover (or -flo)***: Whether the consumer has failover to the destination cluster.
   * ***--sourceRewind (or -srr)***: Whether to rewind the consumer to start from the beginning of the retention period.
   * ***--sourceCluster (or -src)***: The alias of the source cluster specified in the MM2 configuration.
   * ***--destCluster (or -dst)***: The alias of the destination cluster specified in the MM2 configuration.
   * ***--destCluster (or -dst)***: The alias of the destination cluster specified in the MM2 configuration.
   * ***--replicationPolicyClass (or -rpc)***: The class name of the replication policy to use. Works with a custom MM2 replication policy.
   * ***--replicationPolicySeparator (or -rps)***: The separator to use with the DefaultReplicationPolicy between the source cluster alias and the topic name.
   * ***--glueSchemaRegistry (or -gsr)***: Use the AWS Glue Schema Registry. Default **false**.
   * ***--gsrRegion or (-gsrr)***: The AWS region for the AWS Glue Schema Registry. Default ***us-east-1***.
   * ***--gsrRegistryName or (-grn)***: The AWS Glue Schema Registry Name. If not specified, the default registry is used.
   * ***--gsrSchemaName or (-gsn)***: The AWS Glue Schema Registry Schema Name. If not specified, the topic name is used as the schema name.
   * ***--gsrSchemaDescription or (-gsd)***: The AWS Glue Schema Registry Schema description. If not specified, a default one is generated.
   * ***--secondaryDeserializer or (-sdd)***: Enable the secondary deserializer for AWS Glue Schema Registry. Default ***false***.

   
   ### Example usage:
   
   #### At the source and the destination with a background process to sync MM2 checkpointed offsets to the __consumer_offsets internal topic at the destination.
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -mtls -src msksource
   ```

   #### At the destination using the consumer to query and snap to MM2 checkpointed offsets.
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -mtls -flo -src msksource -dst mskdest
   ```

   #### At the source or destination using SASL/SCRAM authentication with a user named "nancy".
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -sse -ssu nancy -src msksource
   ```

   #### At the source or destination using IAM authentication using EC2 instance profile.

   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -iam
   ```