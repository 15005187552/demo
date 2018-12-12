package com.kevin.mapreduce.flowsort;

import com.kevin.mapreduce.FlowCompute;
import com.kevin.mapreduce.FlowPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 根据流量日子处理后结果进行按总流量排序
 * */
public class FlowSumSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FlowSumSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //判断输出路径是否存在 存在就删除
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0:1);
    }
    static  class SortMapper extends Mapper<LongWritable, Text,FlowBean,Text>{
        FlowBean flowBean = new FlowBean();
        Text text = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileds = value.toString().split("\t");
            String phoneNum = fileds[0];
            long upFlow = Long.parseLong(fileds[1]);
            long dFlow = Long.parseLong(fileds[2]);
            flowBean.setUpFlow(upFlow);
            flowBean.setdFlow(dFlow);
            flowBean.setSumFlow(upFlow+dFlow);
            text.set(phoneNum);
            context.write(flowBean,text);
        }
    }

    static class SortReducer extends Reducer<FlowBean ,Text ,Text,FlowBean>{

        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           context.write(values.iterator().next(),bean);
        }
    }
}
