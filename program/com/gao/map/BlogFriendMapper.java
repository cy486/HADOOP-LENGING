package com.gao.map;/*
 * @Author chengpunan
 * @Description //TODO $
 * @Date $ $
 * @Param $
 * @return $
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
* 把数据读入
* 1. 按照：切割。把人和好友分隔开。
* 2、 按照，切割，把好友切割开。
* 3、 把好友作为key，persion 作为value写入
* 4、 进过reduce便可得出结果
* */
public class BlogFriendMapper extends Mapper<LongWritable,Text,Text,Text> {
    protected void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString(); // 读入一行，形如A：B,V，G，L
        System.out.println(line);
        if (line.length() != 0) {
            String[] persons = line.split(":");//第一次切割，把人和朋友分开
            String person = persons[0];
            System.out.println(persons[0]);
            String[] friends = persons[1].split(",");//第二次切割，把朋友分开。
            for (String friend : friends) {
                context.write(new Text(friend), new Text(person));//重新写入
            }
        }
    }
}
