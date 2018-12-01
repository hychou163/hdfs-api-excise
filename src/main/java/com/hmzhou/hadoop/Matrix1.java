package com.hmzhou.hadoop;

import com.hmzhou.hadoop.mapper.Matrix1Mapper;
import com.hmzhou.hadoop.reducer.Matrix1Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by hyjoy on 2018/11/25.
 */
public class Matrix1 {
    private static String inPath = "/matrix/input/matrix2.txt";

    private static String outPath = "/matrix/output";

    private static String hdsf = "hdfs://192.168.31.10:9000";

    private int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", hdsf);
            Job job = Job.getInstance(configuration, "matrix_step1");

            job.setJarByClass(Matrix1.class);
            job.setMapperClass(Matrix1Mapper.class);
            job.setReducerClass(Matrix1Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileSystem fileSystem = FileSystem.get(configuration);

            Path inputPath = new Path(inPath);
            if (fileSystem.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }

            Path outputPath = new Path(outPath);
            fileSystem.delete(outputPath, true);

            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true) ? 1 : -1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) throws IOException {
        int result = new Matrix1().run();

        if (result == 1) {
            System.out.println("运行成功");
        } else {
            System.out.println("运行失败");
        }


    }
}
