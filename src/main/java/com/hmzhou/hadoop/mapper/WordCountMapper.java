package com.hmzhou.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * wordcount map
 * Created by hmzhou on 2018/11/24.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 注意泛型的对应关系
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 此处使用空格分割
        String[] strings = value.toString().split(" ");
        for (String s : strings) {
            context.write(new Text(s), new IntWritable(1));
        }
    }
}
