package com.hmzhou.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by hyjoy on 2018/11/24.
 */
public class HelloWorld {
    public static void main(String[] args) throws IOException {
        String uri = "hdfs://192.168.31.10:9000/";

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), configuration);

        FileStatus[] statuses = fs.listStatus(new Path("/input"));

        for (FileStatus status: statuses) {
            System.out.println(status);
        }
    }
}
