package com.mb.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Objects;

public class Splitter implements FlatMapFunction<String, Tuple2<String,Long>> {
    final static Logger LOGGER = Logger.getLogger(Splitter.class);
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {
        if(Objects.nonNull(value) && value.contains(",")){
            final String[] parts = value.split(",");
            collector.collect(new Tuple2<>(parts[2], Long.parseLong(parts[1])));
        }

    }
}
