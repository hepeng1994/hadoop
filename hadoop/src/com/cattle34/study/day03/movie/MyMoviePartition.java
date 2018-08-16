package com.cattle34.study.day03.movie;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
//按照电影来取膜分组
public class MyMoviePartition extends HashPartitioner<MovieBeen,NullWriter> {
    @Override
    public int getPartition(MovieBeen movieBean, NullWriter value, int numReduceTasks) {
        return (movieBean.getMovie().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
