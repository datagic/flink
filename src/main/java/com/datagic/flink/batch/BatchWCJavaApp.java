package com.datagic.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Desc: 使用Java API 开发 Flink 批处理 word count程序
 * Author 云瞻
 */
public class BatchWCJavaApp {

    public static void main(String[] args) throws Exception{
        String input = "/Users/datagic/Downloads/temp/input";

        // 1. 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        DataSource<String> text = env.readTextFile(input);

        // 3. 处理
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\t");
                if (tokens.length > 0) {
                    for (String token : tokens) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }

}
