package com.atguigu.mapreduce.flowpart;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    //私有化属性
    private Integer upFlow; //上行流量
    private Integer downFlow; //下行流量
    private Integer sumFlow; //总流量

    public FlowBean() {
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Integer sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() { //重写此方法，总流量等于上行+下行，前提是上行和下行传进来
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    //进行序列化
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    //进行反序列化
    public void readFields(DataInput in) throws IOException {
        upFlow=in.readInt();
        downFlow=in.readInt();
        sumFlow=in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
