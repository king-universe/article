package com.learn.bigdata.artist.mr.AristJoinAristUser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Vector;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/15 10:03
 */
public class AristJoinAristUserReducer extends Reducer<Text, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(AristJoinAristUserReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Vector<String> vecA = new Vector<String>();
        Vector<String> vecB = new Vector<String>();
        for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("a:")) {
                vecA.add(val.substring(2));
            } else if (val.startsWith("b:")) {
                vecB.add(val.substring(2));
            }
        }
        for (int i = 0; i < vecA.size(); i++) {
            if (vecB.size() <= 0) {
                context.write(key, new Text(vecA.get(i) + "\t" + "null" + "\t" + "null"));
            } else {
                for (int j = 0; j < vecB.size(); j++) {
                    context.write(key, new Text(vecA.get(i) + "\t" + vecB.get(j)));
                }
            }
        }

    }
}
