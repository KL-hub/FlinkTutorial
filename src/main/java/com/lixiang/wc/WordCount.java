package com.lixiang.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputPath="E:\\idea-project\\FlinkTutorial\\src\\main\\resources\\hello";

        DataSet<String> dataSet = env.readTextFile(inputPath);

        //对数据进行处理，按照空格粉刺展开，转换为（word，1）这样的二元组进行统计
        AggregateOperator<Tuple2<String, Integer>> resultSet = dataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        resultSet.print();



    }
    public static  class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按照空格分词
            String[] words = s.split(" ");
            //遍历所有的word,包装成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }

}
