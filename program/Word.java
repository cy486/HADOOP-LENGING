import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;

public class Word {
    //map过程
    public static class doMapper extends Mapper<Object, Text,Text, IntWritable>{
        private static Text newkey = new Text();
        private static final IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //进行map过程
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                newkey.set(itr.nextToken());
                context.write(newkey,one);
            }
        }
    }
    //reduce过程
    public static class doReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private static IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values){
                sum += value.get();
            }
            result.set(sum);
            context.write(key,result);
            System.out.println("单词"+key+"数量"+result);
        }
    }
//主函数
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = new Job(conf,"WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(doMapper.class);
        job.setReducerClass(doReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path in=new Path("hdfs://165.227.63.36:9000/data/test.txt");
        Path out=new Path("hdfs://165.227.63.36:9000/data/output");
        Path path = new Path("hdfs://165.227.63.36:9000/data/output");// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        FileInputFormat.addInputPath(job,in);
        FileOutputFormat.setOutputPath(job,out);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
