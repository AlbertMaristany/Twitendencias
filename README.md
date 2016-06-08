# Spark Streaming - Twitendencias

IMPORTANT: In scripts folder you will find the script shell to create the corresponding tables in HBase.

Spark streaming job. To execute this job you should start the following tools:
  - Kafka (to queue the tweets)
  - Hadoop (to store the info in hdfs)
  - HBase (to store some relevant information during the analysis of data)
  - Node.js (to view the results in the web page)  --> you should go to the folder of node.js and execute your server. For example "cd myapp". And then "node app".

To execute the job through Eclipse IDE, take in account the following things:
  - Twitendencias/src/main/java/Twitendencias.java
    - (line 26) Your code line should look like: SparkConf conf = new SparkConf().setAppName("Twitendencias").setMaster("local[*]");

To execute the job through Spark directly, take in account the following thing:
  - Twitendencias/src/main/java/Twitendencias.java
    - (line 26) Your code line should look like: SparkConf conf = new SparkConf().setAppName("Twitendencias").setMaster("spark://[IpOfSpark]:[PortOfSpark]");

You can find the most important parts of the code in file: 
  - Twitendencias/src/main/java/Twitendencias.java
  - Twitendencias/src/main/java/Streaming/Stream.java
