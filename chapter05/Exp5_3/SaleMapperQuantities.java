package com.chap05_3.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SaleMapperQuantities extends Mapper<LongWritable, Text, IntWritable, Sale> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        //分词
        String[] words = data.split(",");
        //创建销售对象
        Sale sale = new Sale();
        //设置销售属性
        //产品id
        sale.setProd_id(Integer.parseInt(words[0]));
        //客户id
        sale.setCust_id(Integer.parseInt(words[1]));
        //日期
        sale.setTime(words[2]);
        //渠道id
        sale.setChannel_id(Integer.parseInt(words[3]));
        //促销id
        sale.setPromo_id(Integer.parseInt(words[4]));
        //销售数量
        sale.setQuantity_sold(Integer.parseInt(words[5]));
        //销售总额
        sale.setAmount_sold(Float.parseFloat(words[6]));

        //输出，k2 年份 v2 销售对象
        String year = sale.getTime().split("-")[0];
        context.write(new IntWritable(Integer.parseInt(year)), sale);
    }
}
