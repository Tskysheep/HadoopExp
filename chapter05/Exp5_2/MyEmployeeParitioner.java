package com.chap05_2.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
    /*
    * 建立自己的分区规则：
    *   假如薪资<1500，为低薪。 
    *   假如薪资≥1500，薪资<3000 为中薪。 
    *   假如薪资≥3000，为高薪
     */

public class MyEmployeeParitioner extends Partitioner<IntWritable, Employee> {
    /*
    * numPartition 参数： 建立多少个分区
    *
     */

    @Override
    public int getPartition(IntWritable intWritable, Employee employee, int numPartition) {
        if(employee.getSal() < 1500){
            return 0 % numPartition;
        }else if(employee.getSal() >= 1500){
            return 1 % numPartition;
        }else{
            return 2 % numPartition;
        }
    }
}
