package com.mb.stream;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */
public class StreamingJob
{
    final static Logger LOGGER = Logger.getLogger(StreamingJob.class);

    public static void main( String[] args ) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // environment.enableCheckpointing(5000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "groupId");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.commit.interval.ms","1000");
        properties.setProperty("session.timeout.ms","30000");

        final FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>("temp", new SimpleStringSchema(), properties);
        consumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

        final DataStream<Tuple2<String, Long>> keyedStream = environment.addSource(consumer)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(300))
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String,Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> collector) throws Exception {
                        double sum = 0L;
                        int count = 0;
                        for (Tuple2<String, Long> record : input) {
                            sum += sum + record.f1;
                            count++;
                        }
                        final Tuple2<String, Long> result = input.iterator().next();
                        result.f1 = Math.round((sum / count));
                        collector.collect(result);
                    }
                });
        // sink
        keyedStream.print();

        // execute program
        environment.execute("Flink Streaming Java API Skeleton");

    }
}
