package com.imooc.flink.source;

import com.imooc.flink.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

public class SourceApp {

    public static void main(String[] args) throws Exception{

//创建上下文

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       //test01(env);
        //test02(env);
        //test03(env);
        test04(env);


        env.execute("SourceApp");


    }
    public static void test04(StreamExecutionEnvironment env ) {
        DataStreamSource<Student> source = env.addSource(new StudentSource()).setParallelism(1);
        System.out.println(source.getParallelism());
        source.print();
    }
    public static  void test03(StreamExecutionEnvironment env){
        //DataStreamSource<Access> source = env.addSource(new AccessSource()).setParallelism(1);
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2()).setParallelism(2);

        System.out.println(source.getParallelism());
        source.print();

    }
    public static void test01(StreamExecutionEnvironment env){
        env.setParallelism(5);
        DataStreamSource<String> source=env.socketTextStream("47.115.201.92",9527);
        System.out.println("source..."+ source.getParallelism());   //
        // 接收socket过来的数据，一行一个单词，把PK的过滤掉
        SingleOutputStreamOperator<String> filterStream =source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"pk".equals(value);
            }
        }).setParallelism(4);
        System.out.println("filter..."+filterStream.getParallelism());
        filterStream.print();
    }

    public static void test02(StreamExecutionEnvironment env){
        env.setParallelism(4);
        DataStreamSource<Long> Source = env.fromParallelCollection(
                new NumberSequenceIterator(1, 10), Long.class
        );
        System.out.println("source:"+Source.getParallelism());
        SingleOutputStreamOperator<Long> filterStream = Source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value >= 5;
            }
        }).setParallelism(3);
        System.out.println("filterStream:"+filterStream.getParallelism());
        filterStream.print();


    }

    public static void test05(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "47.115.201.92:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties));

        System.out.println(stream.getParallelism());
        stream.print();




    }
}

