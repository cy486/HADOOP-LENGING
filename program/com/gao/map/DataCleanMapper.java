package com.gao.map;
/*
 * @Author chengpunan
 * @Description //TODO $
 * @Date $ $
 * @Param $
 * @return 数据清洗的map程序
 */

import com.gao.bean.LogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataCleanMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private static Text newkey = new Text(); //用于新的键值对的可以、

    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString(); //获取一行的数据
        LogBean bean = pressLog(line);//用pressLog来解析line的值
        if (!bean.isValied()){//判断是否合法
            return;
        }
        newkey.set(bean.toString());
        context.write(newkey,NullWritable.get());
    }
//用于解析数据的函数
    private LogBean pressLog(String line) {
        LogBean logBean = new LogBean();
        //用空格吧line截取
        String[] fields = line.split(" ");
        if (fields.length > 11){
            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
            }else {
                logBean.setHttp_user_agent(fields[11]);
            }
            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValied(false);
            }
        }else {
            logBean.setValied(false);
        }
        return logBean;

        }
}
