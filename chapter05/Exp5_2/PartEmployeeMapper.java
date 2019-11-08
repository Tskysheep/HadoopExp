package com.chap05_2.v17034460224;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartEmployeeMapper extends Mapper<LongWritable, Text, IntWritable, Employee> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        //分词
        String[] words = data.split(",");
        //创建员工对象
        Employee e = new Employee();
        //设置员工属性
        //员工号
        e.setEmpno(Integer.parseInt(words[0]));
        //姓名
        e.setEname(words[1]);
        //职位
        e.setJob(words[2]);
        //老板号
        try {
            e.setMgr(Integer.parseInt(words[3]));
        }catch (Exception ex){
            e.setMgr(-1);
        }
        //入职日期
        e.setHiredate(words[4]);
        //月薪
        e.setSal(Integer.parseInt(words[5]));
        //奖金
        try {
            e.setComm(Integer.parseInt(words[6]));
        }catch (Exception ex){
            e.setComm(0);
        }
        //部门号
        e.setDeptno(Integer.parseInt(words[7]));
        //输出 k2 部门号  v2 员工对象
        context.write(new IntWritable(e.getDeptno()), e);
    }
}
