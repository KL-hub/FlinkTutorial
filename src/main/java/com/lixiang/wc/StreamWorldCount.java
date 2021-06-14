package com.lixiang.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWorldCount {
    public static void main(String[] args) throws Exception {
        //创建 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputPath="E:\\idea-project\\FlinkTutorial\\src\\main\\resources\\hello";

        DataStream<String> dataStream = env.readTextFile(inputPath);

        //基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        env.execute();

    }
}
