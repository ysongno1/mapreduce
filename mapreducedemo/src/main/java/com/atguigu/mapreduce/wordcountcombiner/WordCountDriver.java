package com.atguigu.mapreduce.wordcountcombiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//本地系统上运行

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1.获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2、关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        //3.关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //在WordcountDriver驱动类中指定Combiner
        job.setCombinerClass(WordCountCombiner.class);
        //将WordcountReducer作为Combiner在WordcountDriver驱动类中指定
        //job.setCombinerClass(WordCountReducer.class);

        //5.设置最终(Reducer)输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.指定当前job的读入数据
        FileInputFormat.setInputPaths(job, new Path("E:\\hadooptest\\input\\hello.txt"));

        //7.指定当前job的读入数据
        FileOutputFormat.setOutputPath(job,new Path("e:\\hadooptest\\output"));

        //8.提交job
        boolean result = job.waitForCompletion(true);//传true和false没有区别  true 打印的多 false 打印的少
        System.exit(result ? 0 : 1);

    }
}
