package com.cattle34.study.day03.topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class MapperDemo extends Mapper <LongWritable,Text,Text,OrderBeen> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //order001,u001,雀巢咖啡,sku2,99.0,2 切分
        //
        String[] split = value.toString().split(",");
        OrderBeen been = new OrderBeen();
        been.setOrdernum(split[0]);
        been.setUID(split[1]);
        been.setProduct(split[2]);
        been.setSku(split[3]);
        been.setPrice(Double.parseDouble(split[4]));
        context.write(new Text(been.ordernum),been );

    }
}
