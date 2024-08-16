package com.test.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TranfromsFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  从元素读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("BOb", "./cart", 1000L),
                new Event("Alice", "./home", 1000L)
        );
        // 1. 实现FlatMapFunction
        stream.flatMap(new MyFlatMap()).print("1");
        // 2. 传入一个Lambda表达式
        stream.flatMap((Event value, Collector<String> out ) ->{
                    if(value.user.equals("Mary") )
                        out.collect(value.url);
                    else if(value.user.equals("BOb")){
                        out.collect(value.user);
                        out.collect(value.url);
                        out.collect(value.timestamp.toString());
                    }
                }).returns(new TypeHint<String>() {})
                .print("2");
        env.execute();

    }
    // 实现一个自定义的FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<Event,String>{

        @Override
        public void flatMap(Event event, Collector<String> out) throws Exception {
            out.collect(event.user);
            out.collect(event.url);
            out.collect(event.timestamp.toString());
        }
    }
}


