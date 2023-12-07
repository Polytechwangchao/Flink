package com.imooc.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class CheckpointApp {
    public static void main(String[] args) throws Exception {
//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 不开启checkpoint：不重启
         *
         * 开启了checkpoint
         * 1) 没有配置重启策略：Integer.MAX_VALUE
         * 2) 如果配置了重启策略，就使用我们配置的重启策略覆盖默认的
         *
         * 重启策略的配置：
         * 1) code
         * 2) yaml
         *
         */
        env.enableCheckpointing(5000);
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置StateBackend
       // env.setStateBackend(new FsStateBackend("file:///D:/workspace/imooc-flink/checkpoints"));




      env.setStateBackend(new FsStateBackend("hdfs://47.115.201.92:8020/imooc-flink-checkpoints"));

        // 自定义设置我们需要的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(5, TimeUnit.SECONDS) // 间隔
        ));

        DataStreamSource<String> source = env.socketTextStream("47.115.201.92", 9527);
        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                /**
                 * 对传入异常数据进行过滤，报提示，并进行重启，重启次数会累加，即：出现达到数量的错误后
                 * job会挂掉
                 */
                if(value.contains("pk")) {
                    throw new RuntimeException("PK哥来了，快跑..");
                } else {
                    return value.toLowerCase();
                }
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0)
                .sum(1)
                .print();


        env.execute("CheckpointApp");
    }
}
