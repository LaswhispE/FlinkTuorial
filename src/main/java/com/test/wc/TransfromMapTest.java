package com.test.wc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 1000L),
                new Event("Alice", "./home", 1000L)
        );
        // 进行转换计算，提取user字段
        // 1. 使用自定义类 ，实现MayFunction接口
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());


        // 2. 使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        // 3. 传入 Lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.user);

        result.print();
        result2.print();
        result3.print();
        env.execute();
    }

    // 自定义mapfunction
    public static class MyMapper implements MapFunction<Event, String>{

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
