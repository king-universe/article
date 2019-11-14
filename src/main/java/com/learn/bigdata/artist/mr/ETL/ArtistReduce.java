package com.learn.bigdata.artist.mr.ETL;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/13 15:59
 */
public class ArtistReduce extends Reducer<NullWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text :
                values) {
            context.write(new Text(text), NullWritable.get());
        }
    }
}
