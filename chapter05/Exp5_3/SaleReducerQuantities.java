package com.chap05_3.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaleReducerQuantities extends Reducer<IntWritable, Sale, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Sale> values, Context context) throws IOException, InterruptedException {
        int quantitu_sold_total = 0;
        for(Sale s : values){
            quantitu_sold_total += s.getQuantity_sold();
        }
        //输出 : k4 年份  v4 年销售数量
        context.write(key, new IntWritable(quantitu_sold_total));
    }
}
