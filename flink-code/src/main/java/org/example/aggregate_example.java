package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class aggregate_example {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Continuous streaming loop source
        DataStream<Tuple2<String, Integer>> scores = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                Tuple2<String, Integer>[] data = new Tuple2[]{
                        new Tuple2<>("Akash", 50),
                        new Tuple2<>("Amit", 30),
                        new Tuple2<>("Akash", 120),
                        new Tuple2<>("Amit", 90),
                        new Tuple2<>("Akash", 80),
                        new Tuple2<>("Amit", 15)
                };

                int index = 0;
                while (isRunning) {
                    ctx.collect(data[index % data.length]);
                    index++;
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // Key by name, window, and return accurate separate averages
        DataStream<Tuple2<String, Double>> averageWithKeys = scores
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageWithKeyAccumulator());

        averageWithKeys.print();
        env.execute();
    }

    /**
     * AggregateFunction<InputType, AccumulatorType, OutputType>
     * Input:       Tuple2<String, Integer>  -> (Name, Score)
     * Accumulator: Tuple3<String, Integer, Integer> -> (RunningSum, RunningCount, LastSeenKey)
     * Output:      Tuple2<String, Double>   -> (Name, CalculatedAverage)
     */
    public static class AverageWithKeyAccumulator
            implements AggregateFunction<Tuple2<String, Integer>, org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String>, Tuple2<String, Double>> {

        @Override
        public org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> createAccumulator() {
            // Track: Sum = 0, Count = 0, Key = "unknown"
            return new org.apache.flink.api.java.tuple.Tuple3<>(0, 0, "");
        }

        @Override
        public org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> add(
                Tuple2<String, Integer> value,
                org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> accumulator) {
            // Safely increment sum, count, and capture the actual incoming key ("Akash" or "Amit")
            return new org.apache.flink.api.java.tuple.Tuple3<>(
                    accumulator.f0 + value.f1,
                    accumulator.f1 + 1,
                    value.f0
            );
        }

        @Override
        public Tuple2<String, Double> getResult(org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> accumulator) {
            // Compute calculation using the isolated data elements
            double average = (double) accumulator.f0 / accumulator.f1;
            return new Tuple2<>(accumulator.f2, average);
        }

        @Override
        public org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> merge(
                org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> a,
                org.apache.flink.api.java.tuple.Tuple3<Integer, Integer, String> b) {
            // Merge handling block
            String validKey = a.f2.isEmpty() ? b.f2 : a.f2;
            return new org.apache.flink.api.java.tuple.Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, validKey);
        }
    }
}
