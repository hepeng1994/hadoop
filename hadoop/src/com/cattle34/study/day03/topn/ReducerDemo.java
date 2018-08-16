package com.cattle34.study.day03.topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class ReducerDemo extends Reducer<Text,OrderBeen,OrderBeen,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<OrderBeen> values, Context context) throws IOException, InterruptedException {
       //封装
        ArrayList<OrderBeen> beens = new ArrayList<>();
        for (OrderBeen value : values) {
            OrderBeen orderBeen = new OrderBeen();
            orderBeen.setProduct(value.product);
            orderBeen.setSku(value.sku);
            orderBeen.setPrice(value.price);
            orderBeen.setUID(value.UID);
            orderBeen.setOrdernum(value.ordernum);
            beens.add(orderBeen);
        }
        //排序
        Collections.sort(beens, new Comparator<OrderBeen>() {
            @Override
            public int compare(OrderBeen o1, OrderBeen o2) {
                return o2.price-o1.price>0?0:-1;
            }
        });
        //输出
        for (int i = 0; i <3; i++) {
            context.write(beens.get(i),NullWritable.get());//NullWritable.get()防止空指针
        }
    }
}
