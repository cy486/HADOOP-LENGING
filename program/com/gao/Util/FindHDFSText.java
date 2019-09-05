package com.gao.Util;/*
 * @Author chengpunan
 * @Description //TODO $
 * @Date $ $
 * @Param $
 * @return $
 */

import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * 读取hdfs上指定文件中的内容
 * @company 源辰信息
 * @author navy
 */
public class FindHDFSText {
    private static Logger log = Logger.getLogger(FindHDFSText.class);// 创建日志记录器

    public static void main(String[] args) {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();// 加载配置文件
            Path p= new Path("hdfs://165.227.63.36:9000/data/output/part-r-00000");
            fs = p.getFileSystem(conf);
             // 默认是读取/user/navy/下的指定文件
            System.out.println("要查看的文件路径为："+fs.getFileStatus(p).getPath());

            FSDataInputStream fsin = fs.open(p);
            byte[] bs = new byte[1024 * 1024];
            int len = 0;
            while((len = fsin.read(bs)) != -1){
                System.out.print(new String(bs, 0, len));
            }

            System.out.println();
            fsin.close();
        } catch (Exception e) {
            log.error("hdfs操作失败!!!", e);
        }
    }
}