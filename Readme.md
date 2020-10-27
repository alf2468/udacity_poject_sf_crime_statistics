# Udacity Project SF Crime Statistics 

## Run project

### Requirements

* Python 3.7
* Spark 2.4.3
* Scala 2.11.x
* Java 1.8.x
* Kafka 2.3.0 build with Scala 2.11.x

### Set up environment

Install requirements
```shell script
pip install -r requirements.txt
```

Start Zookeeper 
```shell script
KAFKA_HOME/bin/zookeeper-server-start.sh kafka_config/zookeeper.properties
```

Start Kafka
```shell script
KAFKA_HOME/bin/kafka-server-start.sh kafka_config/server.properties
```

### Produce data

```shell script
python3 kafka_server.py
```

### Consume data
 
```
python3 consumer_server.py
```

### Create Spark Application

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --master "local[*]" data_stream.py
```

## Questions

**1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?**

I checked the ```processedRowsPerSecond``` value from the Progress Report to indicate if changes increased the 
speed of processing data.


**2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on 
values, how can you tell these were the most optimal?**

* Obviously the ```maxOffsetsPerTrigger``` value increased the throughput by increasing the parameter since it effects on
how many events will be handled at once.
* In the spark session configutration the parameters for ```spark.streaming.kafka.maxRatePerPartition``` and 
```spark.sql.shuffle.partitions``` had a big influence on the number of processed records per seconds. I got the best 
results with a value of 50 for both parameters.




