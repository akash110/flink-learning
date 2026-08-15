package org.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Map_example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // dont use in real life secanrios
        DataStream<Integer> numbers = env.fromElements(1,2,3,4,5,6,7,8,9,10);
        DataStream<Integer> doubled = numbers.map(x->x*2);
        doubled.print();
        env.execute();
    }
}