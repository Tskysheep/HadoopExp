package com.chap05_3.v17034460224;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducerAmounts extends Reducer<IntWritable, Sale, IntWritable, FloatWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Sale> values, Context context) throws IOException, InterruptedException {
        float amount_sold_total = 0.0f;
        for(Sale s : values){
            amount_sold_total += s.getAmount_sold();
        }
        //输出 : k4 年份  v4 年销售数量
        context.write(key, new FloatWritable(amount_sold_total));
    }
}
