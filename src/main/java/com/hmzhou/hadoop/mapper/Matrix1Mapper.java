package com.hmzhou.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hyjoy on 2018/11/25.
 */
public class Matrix1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();


    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowAndLine = value.toString().split("\t");

        String row = rowAndLine[0];
        System.out.println(row);
        String[] lines = rowAndLine[1].split(",");

        for (String line : lines) {
            String column = line.split("_")[0];
            String valueStr = line.split("_")[1];

            // key: 列号 value： 行号——值
            outKey.set(column);
            outValue.set(row + "_" + valueStr);

            context.write(outKey, outValue);
        }
    }
}
