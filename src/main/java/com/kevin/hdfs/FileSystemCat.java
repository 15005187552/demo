package com.kevin.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

public class FileSystemCat {
    public static void main(String[] args) throws IOException {
       // String uri = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://kevin:9000");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path("/kevin/test/a.txt"));
            IOUtils.copyBytes(in,System.out,4096,false);
        }
       catch (Exception e){
           e.printStackTrace();
       }
       finally {
            IOUtils.closeStream(in);
        }
    }
}
