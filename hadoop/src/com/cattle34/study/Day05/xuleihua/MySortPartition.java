package com.cattle34.study.Day05.xuleihua;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MySortPartition extends HashPartitioner<FlowBean, NullWritable> {

    @Override
    public int getPartition(FlowBean flowBean, NullWritable value, int numReduceTasks) {
        return flowBean.getSumFlow() > 6000 ? 0 : 1;
    }
}
