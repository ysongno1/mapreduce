package com.atguigu.mapreduce.flowcompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outK = new FlowBean();
    private Text outV = new Text();


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context) throws IOException, InterruptedException {
        String flow = value.toString();
        String[] split = flow.split("\t");

        outK.setUpFlow(Integer.parseInt(split[1]));
        outK.setDownFlow(Integer.parseInt(split[2]));
        outK.setSumFlow();
        outV.set(split[0]);

        context.write(outK,outV);

    }
}
