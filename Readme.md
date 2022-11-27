# FLINK

### Data Processing Using the DataStream API
Use case – sensor data analytics
Now that we have looked at various aspects of DataStream API, 
let's try to use these concepts to solve a real world use case. 
Consider a machine which has sensor installed on it, and we wish 
to collect data from these sensors and calculate average temperature per sensor every five minutes.

In this scenario, we assume that sensors are sending information 
to Kafka topic called temp with information as (timestamp, temperature, sensor-ID). 
Now we need to write code to read data from Kafka topics and processing it using Flink transformation

How to launch this application ? 
- Download flink and start it 
- mvn clean package 
- start Kafka and publish events from a producer 
- sink produces the results witch are saved in result.txt file
