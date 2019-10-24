package com.ch03.v17034460224;

/**
 * Hello world!
 *
 */
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

public class DeleteHDFSFile
{
    public static void main( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        //配置NameNode地址
        URI uri = new URI("hdfs://172.0.0.30:8020");
        //指定用户名，获取FileSystem对象
        FileSystem fs = FileSystem.get(uri,conf,"hadoop");
        //删除文件
        Path path = new Path("/17034460224/test2.txt");

        fs.delete(path,true);


        fs.close();
    }
}
