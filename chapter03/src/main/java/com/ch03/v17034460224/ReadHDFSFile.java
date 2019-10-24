package com.ch03.v17034460224;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.nio.ByteBuffer;

public class ReadHDFSFile {
    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        //配置NameNode地址
        URI uri = new URI("hdfs://172.0.0.30:8020");
        //指定用户名，获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //读取文件内容
        Path path = new Path("/17034460224/test5.txt");
        FSDataInputStream is = fs.open(path);
        byte[] content = new byte[1024];
        is.read(content);
        //输出结果
        System.out.println(new String(content).toString());

        fs.close();
    }
}
