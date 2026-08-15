package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class key_example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // dont use in real life secanrios
        DataStream<String> values = env.fromElements("Akash","Akash","Amit","Amit","Akash","Amit");
        //DataStream<String> df_transform1 =values.map(x-> Tuple2(x,1));
        DataStream<Tuple2<String, Integer>> df_transform1 = values.map(x -> new Tuple2<>(x, 1))
                .returns(new TypeHint<Tuple2<String, Integer>>(){});

        DataStream<Tuple2<String, Integer>> counts = df_transform1
                .keyBy(x -> x.f0)
                .sum(1);

        counts.print();
        //DataStream<Integer> filtered = numbers.filter(x->x>5);
        //filtered.print();
        env.execute();
    }
}
