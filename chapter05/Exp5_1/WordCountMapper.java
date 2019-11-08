package com.chap05_1.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//泛型 k1 v1 k2 v2
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
        *context 表示Mapper的上下文
        * 上文：HDFS
        * 下文：Mapper
         */
        String data = value.toString();
        //分词
        String[] words = data.split(" ");
        //输出k2 v2
        for(String w:words){
            context.write(new Text(w), new IntWritable(1));
        }
    }
}
