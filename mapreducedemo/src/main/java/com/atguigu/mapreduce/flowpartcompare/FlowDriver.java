package com.atguigu.mapreduce.flowpartcompare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //通过配置文件创建Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //绑定当前Driver
        job.setJarByClass(FlowDriver.class);

        //绑定当前Mapper和Reduce的jar
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //分区
        job.setPartitionerClass(FlowPart.class);
        job.setNumReduceTasks(5);

        //⭐⭐⭐⭐⭐⭐输出类型交换⭐⭐⭐⭐⭐⭐
        //指定Mapper的输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定输入路径
        FileInputFormat.setInputPaths(job,new Path("E:\\hadooptest\\input\\part-r-00000"));

        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadooptest\\part4"));

        //提交运行
        job.waitForCompletion(true);

    }
}
