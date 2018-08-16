package com.cattle34.study.Day05.xuleihua;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private String phoneNum;
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(String phoneNum, long upFlow, long downFlow) {
        this.phoneNum = phoneNum;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phoneNum);
        out.writeLong(this.upFlow);
        out.writeLong(this.downFlow);
//        out.writeLong(this.sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phoneNum = in.readUTF();
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
//        this.sumFlow = in.readLong();
        /*这种方式，可以省去一部分的网络开销*/
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phoneNum='" + phoneNum + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    @Override
    public int compareTo(FlowBean o) {
        return (int) (o.getSumFlow() - this.getSumFlow());
    }
}
