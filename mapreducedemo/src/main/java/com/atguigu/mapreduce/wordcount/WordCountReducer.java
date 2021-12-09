package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {



    IntWritable v = new IntWritable();

    /**
     *
     * @param key //每一组的单词
     * @param values // 集合类型 当前单词对应的所有value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1.累加求和
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }

        //2.输出
        v.set(sum);
        context.write(key,v);

    }
}
