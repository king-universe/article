package com.learn.bigdata.artist.mr.AristJoinAristUser;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/14 17:46
 */
public class AristJoinAristUserMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(AristJoinAristUserMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String line = value.toString();
        if (StringUtils.isEmpty(line)) return;
        if (filePath.indexOf("artist_data_1") != -1) {
            logger.info("文件路径是："+filePath);
            String[] split = line.split("\t");
            if (split.length != 2) return;
            String artistId = split[0];
            String artist_name = split[1];
            context.write(new Text(artistId), new Text("a:" + artist_name));
        } else if (filePath.indexOf("user_artist_data") != -1) {
            String[] split = line.split("\t");
            if (split.length != 3) return;
            String userid = split[0];
            String artistId = split[1];
            String playCount = split[2];
            context.write(new Text(artistId), new Text("b:" + userid + "\t" + playCount));
        }
    }
}
