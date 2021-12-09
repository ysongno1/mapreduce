package com.atguigu.mapreduce.flowpart;

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

        //指定Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定自定义分区器
        job.setPartitionerClass(FlowPart.class);
        //同时指定相应数量的ReduceTask
        job.setNumReduceTasks(5);

        //指定输入路径
        FileInputFormat.setInputPaths(job,new Path("E:\\input\\phone_data.txt"));

        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path("E:/part"));

        //提交运行
        job.waitForCompletion(true);

    }
}
