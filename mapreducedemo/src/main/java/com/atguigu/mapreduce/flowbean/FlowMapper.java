package com.atguigu.mapreduce.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    //想只创建一次对象，但不建议直接在类里面new对象，比如new Text，new FlowBean
    //所以采用Mapper里自带的方法setup来声明对象，只创建一次

    private Text outK;
    private FlowBean outV;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //转换字符串并分割
        String line = value.toString();
        String[] phones = line.split("\t");

        //获取手机后 上行流量 下行流量
        String phone = phones[1];
        String upFlow = phones[phones.length - 3];
        String downFlow = phones[phones.length - 2];

        //进行封装
        outK.set(phone);
        outV.setUpFlow(Integer.parseInt(upFlow));
        outV.setDownFlow(Integer.parseInt(downFlow));
        outV.setSumFlow();

        //写出
        context.write(outK,outV);
    }
}
