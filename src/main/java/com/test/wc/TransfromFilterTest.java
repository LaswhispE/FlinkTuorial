package com.test.wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 1000L),
                new Event("Alice", "./home", 1000L)
        );

        // 1. 传入一个实现了FilterFunction的类的对象
        SingleOutputStreamOperator<Event> result1 = stream.filter(new MyFilter());
        result1.print();
        // 2. 传入一个匿名类实现 FilterFunction接口
        SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("BOb");
            }
        });
        result2.print();
        // 3. 传入一个匿名函数
        stream.filter(data -> data.user.equals("Alice")).print("lamdba: Alice click");


        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event>{

        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Mary");
        }
    }
}

