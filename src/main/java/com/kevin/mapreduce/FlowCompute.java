package com.kevin.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author kevin
 * 对日志信息进行简单的统计
 */
public class FlowCompute {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(FlowCompute.class);

        job.setPartitionerClass(FlowPartition.class);
        job.setNumReduceTasks(5);


        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0:1);
    }
    static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fileds = value.toString().split("\t");
            String phoneNum = fileds[1];
            long upFlow = Long.parseLong(fileds[fileds.length - 3]);
            long dFlow = Long.parseLong(fileds[fileds.length - 2]);
            FlowBean bean = new FlowBean(upFlow, dFlow);
            context.write(new Text(phoneNum), bean);
        }
    }

    static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            Iterator<FlowBean> flowBeans = values.iterator();
            long sumUpFlow = 0;
            long sumdFlow = 0;
            while (flowBeans.hasNext()) {
                FlowBean flowBean = flowBeans.next();
                sumUpFlow += flowBean.getUpFlow();
                sumdFlow += flowBean.getdFlow();
            }
            FlowBean flowBean = new FlowBean(sumUpFlow, sumdFlow);
            context.write(key, flowBean);
        }
    }
}
