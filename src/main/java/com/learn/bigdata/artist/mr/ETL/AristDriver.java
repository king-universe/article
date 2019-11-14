package com.learn.bigdata.artist.mr.ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
public class AristDriver  {
    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(AristDriver.class);

            job.setMapperClass(ArtistMapper.class);

            job.setReducerClass(ArtistReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            //6.指定job的原始输入文件的路径和输出路径
            FileInputFormat.setInputPaths(job,new Path("D:\\tmp\\20151220.log"));
            FileOutputFormat.setOutputPath(job, new Path("D:\\output"));

            //7.提交
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
