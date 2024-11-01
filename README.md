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
   * ***--iamEnable (or -iam)***: Enable AWS IAM authentication between this application and Amazon MSK with in-transit encryption. If this parameter is specified, this parameter cannot be specified with --mTLSEnable (or -mtls) or --sslEnable (or -ssl) or --saslscramEnable (or -sse). For IAM authentication / authorization to work, attach an [authorization policy](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html#create-iam-access-control-policies) to the IAM role that corresponds to EC2 Instance Profile.
   * ***--saslscramUser (or -ssu)***: The name of the SASL/SCRAM user stored in AWS Secrets Manager to be used for SASL/SCRAM authentication between this application and Amazon MSK. If this parameter is specified, --saslscramEnable (or -sse) also needs to be specified.
   * ***--region (or -reg)***: The region for AWS Secrets Manager storing secrets for SASL/SCRAM authentication with Amazon MSK. Default us-east-1.
   * ***--glueSchemaRegistry (or -gsr)***: Use the AWS Glue Schema Registry. Default **false**.
   * ***--gsrRegion or (-gsrr)***: The AWS region for the AWS Glue Schema Registry. Default ***us-east-1***.
   * ***--gsrRegistryName or (-grn)***: The AWS Glue Schema Registry Name. If not specified, the default registry is used.
   * ***--gsrSchemaName or (-gsn)***: The AWS Glue Schema Registry Schema Name. If not specified, the topic name is used as the schema name.
   * ***--gsrSchemaDescription or (-gsd)***: The AWS Glue Schema Registry Schema description. If not specified, a default one is generated.
   * ***--secondaryDeserializer or (-sdd)***: Enable the secondary deserializer for AWS Glue Schema Registry. Default ***false***.

   
   ### Example usage:
   
   #### At the source and the destination with a background process to sync MM2 checkpointed offsets to the __consumer_offsets internal topic at the destination.
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -mtls
   ```


   #### At the source or destination using SASL/SCRAM authentication with a user named "nancy".
   
   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -sse -ssu nancy
   ```

   #### At the source or destination using IAM authentication using EC2 instance profile.

   ```
   java -jar KafkaClickstreamConsumer-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/consumer.properties -nt 3 -rf 10800 -iam
   ```