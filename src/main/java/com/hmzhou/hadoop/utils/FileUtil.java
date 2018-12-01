package com.hmzhou.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hyjoy on 2018/12/1.
 */
public class FileUtil {

    private FileUtil() throws IllegalAccessException {
        throw new IllegalAccessException("u'll not access FileUtil");
    }


    public static boolean deleteFile(Configuration configuration, String dirPath) throws IOException {
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

    public static void cat(Configuration configuration, String filePath) throws IOException {
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
