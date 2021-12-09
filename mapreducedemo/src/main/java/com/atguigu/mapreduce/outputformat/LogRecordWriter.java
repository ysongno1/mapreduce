package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream otherfos;
    private FSDataOutputStream atguigufos;

    //只创建一次对象之构造器写法
    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        atguigufos = fs.create(new Path("E:\\hadooptest\\log\\atguigu.txt"));
        otherfos = fs.create(new Path("E:\\hadooptest\\log\\other.txt"));

    }

    @Override
    //每一个reduce输出就会调用一次
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //转换字符串
        String log = key.toString();
        //判断是否包含atguigu
        if (log.contains("atguigu")) {
            atguigufos.writeBytes(log + "\n");
        } else {
            otherfos.writeBytes(log + "\n");
        }
    }

    @Override
    //关流
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        otherfos.close();
        atguigufos.close();
    }
}
