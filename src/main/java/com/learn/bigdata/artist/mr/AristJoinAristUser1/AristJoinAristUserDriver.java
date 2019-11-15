package com.learn.bigdata.artist.mr.AristJoinAristUser1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/15 10:11
 * 未正常实现
 */
public class AristJoinAristUserDriver {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
            conf = optionparser.getConfiguration();
            Job job = Job.getInstance(conf, "leftjoin");

            job.setJarByClass(AristJoinAristUserDriver.class);

            FileInputFormat.addInputPaths(job, conf.get("input_dir"));
            Path out = new Path(conf.get("output_dir"));
            FileOutputFormat.setOutputPath(job, out);
            job.setNumReduceTasks(conf.getInt("reduce_num", 2));

            job.setInputFormatClass(TextInputFormat.class); //设置文件输入格式
            job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格式

            job.setMapperClass(AristJoinAristUserMapper.class);

            job.setReducerClass(AristJoinAristUserReducer.class);


            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ArtistCombineValues.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

           /* //6.指定job的原始输入文件的路径和输出路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setNumReduceTasks(conf.getInt("reduce_num", 3));

*/

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
