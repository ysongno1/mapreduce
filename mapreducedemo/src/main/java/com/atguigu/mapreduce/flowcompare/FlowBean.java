package com.atguigu.mapreduce.flowcompare;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 全排序 只有一个分区 最终输出结果只有一个文件,且文件内部有序
/**
 * 实现WritableComparable接口重写compareTo方法
 */
public class FlowBean implements WritableComparable<FlowBean> {
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
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }



    @Override
    public int compareTo(FlowBean o) {
        //按照总流量比较,倒序排列
        if (this.sumFlow > o.sumFlow) {
            return -1;
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            if (this.upFlow > o.upFlow) {
                return 1;
            } else if (this.upFlow < o.upFlow) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
