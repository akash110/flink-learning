package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class tumbling_window_example {

    // Define the record layout
    public static class Event {
        public String value;
        public int count;
        public long timestamp;

        public Event() {}   // <-- add this

        public Event(String value, int count, long timestamp) {
            this.value = value;
            this.count = count;
            this.timestamp = timestamp;
        }


        public String getName() {
            return value;
        }
        public int getCount() {
            return count;
        }
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Event{value='" + value + "', count=" + count + ", timestamp=" + timestamp + "}";
        }

    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // dont use in real life secanrios

        DataStream<Event> eventStream = env.fromElements(
                new Event("Rahul", 100, 2000),  // Window 1: 0ms to 4999ms
                new Event("Rahul", 100, 5000),  // Window 2: 5000ms to 9999ms
                new Event("Rahul", 100, 8000),  // Window 2
                new Event("Rahul", 100, 12000), // Window 3: 12000ms to 14999ms
                new Event("Rahul", 100, 15000)  // Window 4: 15000ms+
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp())
        );

        DataStream<Event> aggregates=eventStream.keyBy(event->event.getName())
                   .window(
                           TumblingEventTimeWindows.of(Time.seconds(10))
                   )
                  .sum("count");

        // Map the Event object into a clean Tuple3 (String, Integer, Long)
        //DataStream<Tuple3<String, Integer, Long>> extractedFields = eventStream.map(
        //        x -> new Tuple3<>(x.value, x.count, x.timestamp)
        //).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG)); // Helps Flink optimize type extraction

        // This prints beautifully as: (Rahul, 100, 2000)
        //extractedFields.print();


        //DataStream<Integer> filtered = numbers.filter(x->x>5);
        aggregates.print();
        env.execute();
    }
}
