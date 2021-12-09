package com.atguigu.mapreduce.wordcountcombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner组件的父类是Reducer
 * Combiner和Reducer的区别在于运行的位置
 *      Combiner实在每一个MapTask所在的节点运行
 *      Reducer是接收全局所有Mapper的输出结果
 * Combiner能够运用的前提是不能影响最终的业务逻辑
 */

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        context.write(key,outV);
    }
}
