package com.chap05_4.v17034460224;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, Log> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] words = data.split("\t");
        Log log = new Log();
        //设置内容
        log.setContent(words[2]);
        //url 顺序
        log.setUrl_range(Integer.parseInt(words[3].split(" ")[0]));
        //点击顺序
        log.setClick_range(Integer.parseInt(words[3].split(" ")[1]));
        context.write(new Text(log.getContent()), log);
    }
}
