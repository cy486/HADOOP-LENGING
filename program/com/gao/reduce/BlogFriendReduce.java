package com.gao.reduce;/*
 * @Author chengpunan
 * @Description //TODO 博客共同好友reduce的过程$
 * @Date $ $2019.09.03
 * @Param $
 * @return $
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BlogFriendReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           StringBuilder sb = new StringBuilder();
        for (Text person:values){
            sb.append(person).append(",");
        }
        context.write(key,new Text(sb.toString()));
        }
}
