package com.hmzhou.hadoop;


import com.hmzhou.hadoop.mapper.WordCountMapper;
import com.hmzhou.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hyjoy on 2018/11/24.
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        rmr(configuration, otherArgs[otherArgs.length - 1]);
        Job job = Job.getInstance(configuration, "WordsCount");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        if (job.waitForCompletion(true)) {
            cat(configuration, otherArgs[1] + "/part-r-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }
    }


    private static boolean rmr(Configuration configuration, String dirPath) throws IOException {
        boolean delResult = false;
        Path targetPath = new Path(dirPath);
        FileSystem fs = targetPath.getFileSystem(configuration);
        if (fs.exists(targetPath)) {

            delResult = fs.delete(targetPath, true);
            if (delResult) {
                System.out.println(targetPath + " has bean deleted successful.");
            } else {
                System.out.println(targetPath + " deletion failed.");
            }
        }

        return delResult;
    }

    private static void cat(Configuration configuration, String filePath) throws IOException {
        InputStream inputStream = null;
        Path file = new Path(filePath);
        FileSystem fileSystem = file.getFileSystem(configuration);
        try {
            inputStream = fileSystem.open(file);
            org.apache.hadoop.io.IOUtils.copyBytes(inputStream, System.out, 4096, true);
        } finally {
            if (inputStream != null) {
                org.apache.hadoop.io.IOUtils.closeStream(inputStream);
            }
        }
    }
}
