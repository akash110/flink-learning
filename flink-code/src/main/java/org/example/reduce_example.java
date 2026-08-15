package org.example;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class reduce_example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. Stream of people with different scores/amounts
        DataStream<Tuple2<String, Integer>> scores = env.fromElements(
                new Tuple2<>("Akash", 50),
                new Tuple2<>("Amit", 30),
                new Tuple2<>("Akash", 120),  // Higher than 50
                new Tuple2<>("Amit", 90),    // Higher than 30
                new Tuple2<>("Akash", 80),   // Lower than 120
                new Tuple2<>("Amit", 15)     // Lower than 90
        );

        // 2. Key by name (f0) and reduce to find the MAX score
        DataStream<Tuple2<String, Integer>> maxScores = scores
                .keyBy(x -> x.f0)
                .reduce((currentMax, incomingRecord) -> {
                    // Compare the integer field (f1) of both records
                    if (incomingRecord.f1 > currentMax.f1) {
                        return incomingRecord; // This becomes the new max
                    } else {
                        return currentMax;     // Keep the old max
                    }
                });

        maxScores.print();
        env.execute();
    }
}