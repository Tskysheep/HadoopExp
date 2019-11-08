package com.chap05_1.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /*
         *context 是reduce的上下文
         */
        //对v3求和
        int total = 0;
        for(IntWritable v:values){
            total += v.get();
        }
        //输出：k4 单词  v4 频率
        context.write(key,new IntWritable(total));
    }
}
