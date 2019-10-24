package com.ch03.v17034460224;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class DownloadHDFSFile {
    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        //配置NameNode地址
        URI uri = new URI("hdfs://172.0.0.30:8020");
        //指定用户名，获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //下载文件
        Path src = new Path("/17034460224/test5.txt");
        Path dest = new Path("./");

        fs.copyToLocalFile(src,dest);


        fs.close();
    }
}
