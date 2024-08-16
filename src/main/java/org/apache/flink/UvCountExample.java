package org.apache.flink;

import com.test.wc.ClickSource;
import com.test.wc.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成(设置的是时间阈值,允许最大乱序时间2s)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        // 提取时间戳
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.print("data");
        // 使用AggregateFunction 和 ProcessWindowFunction结合计算UV,A进行增量聚合,P进行输出结果


        // ture作常量键表示所有数据都在一个分区

        stream.keyBy(data -> true)
                // 滚动时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // Agg处理后结果传递给Con
                .aggregate(new UvAgg(), new UvCountResult())
                .print();

        env.execute();
    }
    // 自定义实现AggregateFunction，增量聚合计算NV值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long>{

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> strings) {
            strings.add(event.user);
            // 不返回状态相当于没累加
            return strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return (long)strings.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }



    // 实现自定义的ProcessWIndowFunction ，包装窗口信息
    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {


        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> collector) throws Exception {
            Long uv = elements.iterator().next();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            collector.collect("窗口： " + new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " UV值为： " + uv);
        }
    }
}

