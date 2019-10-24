package com.ch03.v17034460224;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ReadHDFSFileAttr {
    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        //配置NameNode地址
        URI uri = new URI("hdfs://172.0.0.30:8020");
        //指定用户名，获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //写入属性
        Path path = new Path("/17034460224/test5.txt");
        String name = "user.id";
        byte[] value = new String("17034460224").getBytes();
        fs.setXAttr(path,name,value);

        //读取属性
        byte[] ret_value;
        ret_value = fs.getXAttr(path,name);

        //输出结果
        System.out.println(new String(ret_value).toString());


        fs.close();
    }
}
