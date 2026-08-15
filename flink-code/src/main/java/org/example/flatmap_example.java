package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flatmap_example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // don't use in real life scenarios

        DataStream<String> values = env.fromElements("Hello World", "Hello Akash", "Hello All");

        DataStream<String> words = values.flatMap((String line, Collector<String> out) -> {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }).returns(String.class);

        DataStream<String> words1 =words.map(x->"Hello =" + x);

        DataStream<String> words2 =words.filter(x->x.contains("Akash"));

        words2.print();
        env.execute();
    }
}