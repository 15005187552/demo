package com.kevin.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsTest {
    private  static  final String HADOOP_URL="hdfs://kevin:9000";
    private Configuration configuration;
    public HdfsTest(){
        this.configuration = new Configuration();
    }
    public static void main(String[] args) throws IOException {
        HdfsTest hdfsTest = new HdfsTest();
        //在hdfs上创建目录
     //   hdfsTest.createDir("/hdemo");
        //从本地上传文件到hdfs上
       //  hdfsTest.putFile("/root/file.txt","/kevin/test");
        hdfsTest.browserFile("/kevin/test/");
    }
    //创建目录
    public  void createDir(String folder){
      Path path =  new Path(folder);
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            if (!fileSystem.exists(path)){
                fileSystem.create(path);
                System.out.println("目录创建成功。。。。。。。。。。");
            }
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //上传文件 :将本地文件上传到hdfs上
    public void putFile(String local,String remote) throws IOException {
        FileSystem fs= FileSystem.get(configuration);
        fs.copyFromLocalFile(new Path(local),new Path(remote));
        System.out.println("本地文件上传成功。。。。。。。。。");

    }

    //遍历文件
    public void browserFile(String folder) throws IOException {
        FileSystem fs= FileSystem.get(configuration);
        FileStatus[] listStatus = fs.listStatus(new Path(folder));
        System.out.println("start ls:");
        for (FileStatus status:listStatus){
            System.out.println(
                    "name: %s , folder: %s  , size: %d"+ status.getPath() + status.isFile() + status.getLen()
            );
        }
        System.out.println(" end ls");
    }

}
