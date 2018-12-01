package com.hmzhou.hadoop;

import com.hmzhou.hadoop.mapper.Matrix2Mapper;
import com.hmzhou.hadoop.reducer.Matrix2Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by hyjoy on 2018/11/25.
 */
public class Matrix2 {
    private static String inPath = "/matrix/input/matrix1.txt";

    private static String outPath = "/matrix/step2/output/";
    // matrix2 缓存
    private static String cache = "/matrix/output/part-r-00000";

    private static String hdsf = "hdfs://192.168.31.10:9000";

    private int run() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", hdsf);
            Job job = Job.getInstance(configuration, "matrix_step2");
            job.addCacheArchive(new URI(cache + "#matrix2"));

            job.setJarByClass(Matrix2.class);
            job.setMapperClass(Matrix2Mapper.class);
            job.setReducerClass(Matrix2Reducer.class);

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
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = new Matrix2().run();

        if (result == 1) {
            System.out.println("运行成功");
        } else {
            System.out.println("运行失败");
        }
    }
}
