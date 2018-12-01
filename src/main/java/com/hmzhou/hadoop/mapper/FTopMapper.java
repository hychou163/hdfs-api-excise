package com.hmzhou.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by hyjoy on 2018/12/1.
 */
public class FTopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    private Text mKey = new Text();
    private IntWritable mValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // tom hello hadoop cat
        String[] strings = StringUtils.split(value.toString(), ' ');

        for (String string : strings) {
            mKey.set(getFTop(strings[0], string));
            mValue.set(0);

            context.write(mKey, mValue); // 直接关系

            for (String string1 : strings) {
                mKey.set(getFTop(string, string1));
                mValue.set(1);

                context.write(mKey, mValue); // 间接关系
            }
        }
    }

    private String getFTop(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }

        return s2 + ":" + s1;
    }
}
