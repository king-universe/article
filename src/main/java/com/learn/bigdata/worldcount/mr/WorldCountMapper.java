package com.learn.bigdata.worldcount.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/13 16:30
 */
public class WorldCountMapper extends  Mapper<LongWritable, Text, Text, IntWritable>{
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        //1.获取一行的数据
        String valueLine = value.toString();
        //2.按照分隔符对他分开
        String[] words = valueLine.split(" ");
        //3输出
        for (String word : words) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
