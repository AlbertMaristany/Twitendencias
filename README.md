# Spark Streaming - Twitendencias

Spark streaming job. To execute this job you should start the following tools:
  - Kafka (to queue the tweets)
  - Hadoop (to store the info in hdfs)
  - HBase (to store some relevant information during the analysis of data)
  - Node.js (to view the results in the web page)

To execute the job through Eclipse IDE, take in account the following things:
  - Twitendencias/src/main/java/Twitendencias.java
    - (line 26) Your code line should look like: SparkConf conf = new SparkConf().setAppName("Twitendencias").setMaster("local[*]");

To execute the job through Spark directly, take in account the following thing:
  - Twitendencias/src/main/java/Twitendencias.java
    - (line 26) Your code line should look like: SparkConf conf = new SparkConf().setAppName("Twitendencias").setMaster("spark://[IpOfSpark]:[PortOfSpark]");

You can find the most important part of the code in file: 
  - Twitendencias/src/main/java/Twitendencias.java
  - Twitendencias/src/main/java/Streaming/Stream.java
