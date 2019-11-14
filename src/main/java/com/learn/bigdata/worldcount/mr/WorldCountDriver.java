package com.learn.bigdata.worldcount.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/13 16:03
 */
public class WorldCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        //1.获取配置信息
        Job job = Job.getInstance(conf);
        //2.制定jar包所在的本地路径
        job.setJarByClass(WorldCountDriver.class);

        //3.关联mapper/reducer业务类
        job.setMapperClass(WorldCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.制定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.指定最终输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.指定job的原始输入文件的路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
