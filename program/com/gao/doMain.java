package com.gao;/*
 * @Author chengpunan
 * @Description //TODO $
 * @Date $ $
 * @Param $
 * @return $
 */

import com.gao.map.BlogFriendMapper;
import com.gao.map.DataCleanMapper;
import com.gao.reduce.BlogFriendReduce;
import com.gao.reduce.DataCleanReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class doMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "filter");
        job.setJarByClass(doMain.class);
        job.setMapperClass(BlogFriendMapper.class);
        job.setReducerClass(BlogFriendReduce.class);
        job.setOutputKeyClass(Text.class);
        Path in = new Path("hdfs://165.227.63.36:9000/data/blogfriend.txt");
        Path out = new Path("hdfs://165.227.63.36:9000/data/output");
        Path path = new Path("hdfs://165.227.63.36:9000/data/output");// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
