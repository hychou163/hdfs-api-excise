package com.hmzhou.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hyjoy on 2018/11/25.
 */
public class Matrix2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    private List<String> cacheList = new ArrayList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // 通过输入流将全局缓存中的右侧居中读入List<String>
        FileReader fileReader = new FileReader("matrix2");
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        //
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            cacheList.add(line);
        }

        fileReader.close();
        bufferedReader.close();
    }

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row_matrix1 = value.toString().split("\t")[0];
        String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");
        for (String line : cacheList) {
            String row_matrix2 = line.toString().split("\t")[0];
            String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");

            int result = 0;
            for (String column_value_matrix1 : column_value_array_matrix1) {
                String column_matrix1 = column_value_matrix1.split("_")[0];
                String value_matrix1 = column_value_matrix1.split("_")[1];

                // 遍历右侧矩阵每一行的每一列
                for (String column_value_matrix2 : column_value_array_matrix2) {
                    if (column_value_matrix2.trim().startsWith(column_matrix1.trim() + "_")) {
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        result += Integer.valueOf(value_matrix1.trim()) * Integer.valueOf(value_matrix2.trim());
                    }
                }
            }

            outKey.set(row_matrix1.trim());
            outValue.set(row_matrix2.trim() + "_" + result);
            context.write(outKey, outValue);
        }
    }
}
