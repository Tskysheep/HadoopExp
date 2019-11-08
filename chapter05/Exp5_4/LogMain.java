package com.chap05_4.v17034460224;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogMain {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(LogMain.class);
        //指定job的mapper和输出类型 k2 v2
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Log.class);

        //指定job的reducer 和输出类型
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(Log.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job 的输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //执行任务
        job.waitForCompletion(true);

    }
}
