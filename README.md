# RSVP-Event-Streaming

Meetup is a platform for finding and building local communities. People use Meetup to meet new people, learn new things, find support, get out of their comfort zones, and pursue their passions, together.


Every second, huge amount of RSVP event data is generated from Meetup. In this project, I worked with Cloudera to design, develop, test and monitor ETL process. Kafka is implemented to build data source. Spark DataFrame transformation and Impala are for data cleaning and data analytics. HDFS and Kudu are used for data storage, while YARN is implemented for monitoring all jobs. 

## Source Code for RSVP-Event-Streaming

### RSVP-Event-Streaming/bash

Stores all bash commands to excute scala files after `mvn compile`.

### RSVP-Event-Streaming/src/main/scala/data/enigneer

Stores all scala files related to data source, data cleaning, data analytics and data storage.

## POC files

### POC-hive-hands-on

Basic hive operations including partitioned table creation with different methods and data type cast when importing data.

### POC-hive-ops

Distinguished HDFS(Hadoop Distributed File System) and Linux File System. Created Hive managed tables and external tables with different file formats.

### POC-hive-problem-solving

A POC example to implement hive to contribute to business insight. 

### POC-spark-hands-on

Converted a set of data values in a given format stored in HDFS into new data values or a new data format and write them into HDFS.

### POC-spark-problem-solving

A POC example to implement spark to contribute to business insight. 

### POC-sqoop

Imported all tables from MariaDB to hdfs in multiple different file formats. Basic parquet-tools and avro-tools operations are also included here. 
