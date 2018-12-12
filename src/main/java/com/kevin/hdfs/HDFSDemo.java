package com.kevin.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSDemo {
     Configuration conf = null;
      FileSystem fs = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();

        fs = FileSystem.get(new URI("hdfs://kevin:9000"),conf,"root");
    }

    /*
     * 使用流的方式从本地上传文件到hdfs
     * */
    @Test
    public void testUpload() throws IOException {
        {
            FileInputStream inputStream = new FileInputStream("C:\\Users\\Administrator\\Desktop\\日常\\test.txt");
            FSDataOutputStream fsDataOutputStream = fs.create(new Path("/test"),false);
            IOUtils.copy(inputStream, fsDataOutputStream);
        }
    }
    /*
    * 使用流的方式从下载文件到本地
    * */
    @Test
    public  void testDownload() throws IOException {
      FSDataInputStream inputStream = fs.open(new Path("/input/test.txt"));
        FileOutputStream fileOutputStream =new FileOutputStream("G:/a.txt");
        IOUtils.copy(inputStream, fileOutputStream);
    }
    /*
    * 随机读取：从文件的某个位置开始读
    * */
    @Test
    public void testRandomAccess() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/input/test.txt"));
       inputStream.seek(20);
        FileOutputStream fileOutputStream =new FileOutputStream("G:/b.txt");
        IOUtils.copy(inputStream, fileOutputStream);
    }
}
