package com.learn.bigdata.movie.mr.movieMappeJoinRatting;

import com.learn.bigdata.artist.mr.AristJoinAristUser.AristJoinAristUserDriver;
import com.learn.bigdata.movie.mr.movieReduceJoinRatting.MovieJoinRattingMapper;
import com.learn.bigdata.movie.mr.movieReduceJoinRatting.MovieJoinRattingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/18 15:21
 */
@SuppressWarnings("all")
public class MovieJoinRattingDriver {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(MovieJoinRattingDriver.class);
            job.setMapperClass(MovieJoinRattingMapper.class);

            //由于不需要Reduce,所以设置Reduce的任务为0
            job.setNumReduceTasks(0);

            //设置Mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //但是由于要多个文件做关联,所以要添加本地的缓存
            job.addCacheFile(new URI(args[0]));

            //设置输入和输出路径

            FileInputFormat.setInputPaths(job,args[1]);

            Path outPath = new Path(args[2]);
            FileOutputFormat.setOutputPath(job,outPath);
            FileSystem fs = FileSystem.get(conf);

            //7.提交
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
