package com.learn.bigdata.worldcount.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/13 16:31
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> value,
                          Context context) throws IOException, InterruptedException {
        //1.汇总各个key的个数
        int sum=0;
        for (IntWritable intWritable : value) {
            sum+=(intWritable.get());
        }
        //2.输出
        context.write(key, new IntWritable(sum));
    }
}
