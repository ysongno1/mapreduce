package com.atguigu.mapreduce.flowpart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 */

public class FlowPart extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        String s = phone.substring(0, 3);
        if ("136".equals(s)){
            return 0;
        }else if ("137".equals(s)){
            return 1;
        }else if ("138".equals(s)){
            return 2;
        }else if ("139".equals(s)){
            return 3;
        }
        return 4;
    }
}
