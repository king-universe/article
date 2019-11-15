package com.learn.bigdata.movie.mr.movieJoinRatting;

import com.learn.bigdata.artist.mr.AristJoinAristUser.AristJoinAristUserDriver;
import com.learn.bigdata.artist.mr.AristJoinAristUser.AristJoinAristUserMapper;
import com.learn.bigdata.artist.mr.AristJoinAristUser.AristJoinAristUserReducer;
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
 * @date 2019/11/15 14:41
 */
@SuppressWarnings("all")
public class MovieJoinRattingDriver {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
            conf = optionparser.getConfiguration();
            Job job = Job.getInstance(conf,"leftjoin");

            job.setJarByClass(AristJoinAristUserDriver.class);

            FileInputFormat.addInputPaths(job, conf.get("input_dir"));
            Path out = new Path(conf.get("output_dir"));
            FileOutputFormat.setOutputPath(job, out);
            job.setNumReduceTasks(conf.getInt("reduce_num",2));

            job.setMapperClass(MovieJoinRattingMapper.class);

            job.setReducerClass(MovieJoinRattingReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);



            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

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
