package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

public class process_window_example {

    public static class Event {
        public String value;
        public int count;
        public long timestamp;

        public Event() {}

        public Event(String value, int count, long timestamp) {
            this.value = value;
            this.count = count;
            this.timestamp = timestamp;
        }

        public String getName() { return value; }
        public int getCount() { return count; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return "Event{value='" + value + "', count=" + count + ", timestamp=" + timestamp + "}";
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Event> eventStream = env.fromElements(
                new Event("Rahul", 100, 2000),
                new Event("Rahul", 100, 5000),
                new Event("Rahul", 100, 8000),
                new Event("Rahul", 100, 12000),
                new Event("Rahul", 100, 15000)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
        );

        // REWRITE: Using .process() instead of .sum()
        DataStream<String> aggregates = eventStream
                .keyBy(Event::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new TotalSumProcessFunction());

        aggregates.print();
        env.execute();
    }

    /**
     * ProcessWindowFunction<InputType, OutputType, KeyType, WindowType>
     */
    public static class TotalSumProcessFunction
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) {
            int totalSum = 0;

            // Iterate over ALL elements that collected inside this 10-second window block
            for (Event event : elements) {
                totalSum += event.getCount();
            }

            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();

            // Emit a descriptive text string out of the window
            out.collect(String.format("Key: %s | Window: [%d - %d] | Total Count: %d",
                    key, windowStart, windowEnd, totalSum));
        }
    }
}
