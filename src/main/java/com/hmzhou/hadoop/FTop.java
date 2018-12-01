package com.hmzhou.hadoop;

import com.hmzhou.hadoop.mapper.FTopMapper;
import com.hmzhou.hadoop.reducer.TopReducer;
import com.hmzhou.hadoop.utils.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 好友推荐
 * Created by hyjoy on 2018/12/1.
 */
public class FTop {

    private static String inPath = "/data/ftop/input/ftop.txt";

    private static String outPath = "/matrix/output";

    private static String hdsf = "hdfs://master:9000";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdsf);

        FileUtil.deleteFile(configuration, outPath);
        Job job = Job.getInstance(configuration, "FTOP");

        job.setJarByClass(FTop.class);
        job.setMapperClass(FTopMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TopReducer.class);

        // configuration
        Path inputPath = new Path(inPath);
        FileInputFormat.addInputPath(job, inputPath);

        Path outputPath = new Path(outPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // submit
        job.waitForCompletion(true);
    }
}
