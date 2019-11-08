package com.chap05_2.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



public class PartEmployeeReducer extends Reducer<IntWritable, Employee, IntWritable, Employee> {

    @Override
    protected void reduce(IntWritable key, Iterable<Employee> values, Context context) throws IOException, InterruptedException {
        /*
        * key 部门号
        * values 部门的员工
         */

        for(Employee e : values){
            context.write(key,e);
        }

    }
}
