package com.atguigu.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//一次读一行文本，然后将该行的起始偏移量作为key，行内容作为value返回。
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Mapper泛型：两个输入 两个输出<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    //LongWritable 偏移量 字节数 包括回车（回车含两个字符）
    //TextInputFormat extends FileInputFormat<LongWritable, Text> 写死了Map输出

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    /**
     *
     * @param key //偏移量
     * @param value//都进来的一行数据
     * @param context  //全局的上下文传输对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行，将传进来的数据转换为String
        String line = value.toString();

        //2.切割 按照空格
        String[] words = line.split(" ");

        //3.将数据循环并写出
        for (String word : words) {

//           导致每次循环都要重新创建对象，因每次只需改变值 所以改成全局声明
//            Text k = new Text();
                k.set(word);
//            IntWritable v = new IntWritable(1);

            context.write(k, v);
        }
    }

}
