package com.mb.stream;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.util.Objects;

public class CustomWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {


    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(String value, long l) {
        if(Objects.nonNull(value) && value.contains(",")){
            final String[] parts = value.split(",");
            return new Watermark(Long.parseLong(parts[0]));
        }
        return null;
    }

    @Override
    public long extractTimestamp(String value, long l) {
        if(Objects.nonNull(value) && value.contains(",")){
            final String[] parts = value.split(",");
            return Long.parseLong(parts[0]);
        }
        return 0;
    }
}
