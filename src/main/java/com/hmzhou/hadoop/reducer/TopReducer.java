package com.hmzhou.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hyjoy on 2018/12/1.
 */
public class TopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable mValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //
        int flg = 0;
        int sum = 0;
        for (IntWritable value : values) {
            if (value.get() == 0) {// 直接关系
                flg = 1;
            }
            sum += value.get(); // 添加间接权重
        }

        if (flg == 0) {
            mValue.set(sum);
            context.write(key, mValue);
        }
    }
}
