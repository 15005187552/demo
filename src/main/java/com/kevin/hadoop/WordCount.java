package com.kevin.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author kevin
 * @date 2018-12-7
 * 使用MapReduce计算文本中的单词数
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //指定本程序的jar所在的本地路径
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
//设置mapper的输出结果类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//设置reducer的输出结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务所需文件的位置
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置任务完成后保存文件的位置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //阻塞等待任务完成
        boolean res = job.waitForCompletion(true);
        // 0：任务执行成功退出  1：任务执行失败退出
        System.exit(res ? 0 : 1);
    }

    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
