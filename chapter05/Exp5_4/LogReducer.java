package com.chap05_4.v17034460224;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, Log, Log, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Log> values, Context context) throws IOException, InterruptedException {
        for(Log l : values){
            if(l.getUrl_range() == 2 && l.getClick_range() == 1) context.write(l,NullWritable.get());
        }
    }
}
