package com.chap05_3.v17034460224;

import com.chap05_2.v17034460224.Employee;
import com.chap05_2.v17034460224.MyEmployeeParitioner;
import com.chap05_2.v17034460224.PartEmployeeMapper;
import com.chap05_2.v17034460224.PartEmployeeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SaleMain {
    public static void main(String[] args) throws Exception{
        // 创建一个job 工作1
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(SaleMain.class);

        //指定job的mapper和输出的类型 k2 v2
        job.setMapperClass(SaleMapperQuantities.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Sale.class);

        //指定job的reducer和输出的 类型k4 v4
        job.setReducerClass(SaleReducerQuantities.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);

        //指定job的输入和输出类路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行任务
        job.waitForCompletion(true);
//-----------------------------------------------------------//
        // 创建一个job 工作2
        Job job1 = Job.getInstance(new Configuration());
        job1.setJarByClass(SaleMain.class);

        //指定job的mapper和输出的类型 k2 v2
        job1.setMapperClass(SaleMapperAmounts.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Sale.class);

        //指定job的reducer和输出的 类型k4 v4
        job1.setReducerClass(SaleReducerAmounts.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputKeyClass(FloatWritable.class);

        //指定job的输入和输出类路径
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        //执行任务
        job1.waitForCompletion(true);
    }

}
