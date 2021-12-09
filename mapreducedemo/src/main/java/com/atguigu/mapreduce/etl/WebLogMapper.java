package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 清理的过程往往只需要运行Mapper程序，不需要运行Reduce程序。
 */

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();
        //2.切割
      //  String[] split = line.split(" ");

        //3.ETL 把切割封装
        boolean result = parseLog(line,context);

        // 4 日志不合法退出
        if (!result) {
            return;
        }

        // 5 日志合法就直接写出
        context.write(value, NullWritable.get());


    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        String[] fields = line.split(" ");

        //判断日志的长度是否大于11
        if (fields.length>11){
            return true;
        }else {
            return false;
        }
    }
}
