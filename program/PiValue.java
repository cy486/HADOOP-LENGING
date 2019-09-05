import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PiValue {
    //map过程
    public static class doMapper extends Mapper<Object, Text,Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                int pointNum = Integer.parseInt(value.toString());
                HaltonSequence.HaltonSequence1 haltonseq=new HaltonSequence.HaltonSequence1(1000);
                for(int i = 0; i < pointNum; i++){
                    // 取随机数
                    double x=haltonseq.nextPoint()[0]-0.5;
                    double y=haltonseq.nextPoint()[1]-0.5;
                    IntWritable result = new IntWritable(0);
                    if(x*x+y*y<0.25){
                        result = new IntWritable(1);
                    }
                    context.write(value, result);
                }
            }
    }
    //reduce过程
    public static class doReduce extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double pointNum = Double.parseDouble(key.toString());
            double sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum/pointNum*4);
            context.write(key, result);
        }
    }
    //主函数
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job = new Job(conf,"Pi");
        job.setJarByClass(PiValue.class);
        job.setMapperClass(PiValue.doMapper.class);
        job.setReducerClass(PiValue.doReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path in=new Path("hdfs://165.227.63.36:9000/data/pi.txt");
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
