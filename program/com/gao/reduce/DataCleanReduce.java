package com.gao.reduce;/*
 * @Author chengpunan
 * @Description //TODO $
 * @Date $ $
 * @Param $
 * @return $
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
    public  class DataCleanReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

