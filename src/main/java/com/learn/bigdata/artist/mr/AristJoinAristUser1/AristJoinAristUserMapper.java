package com.learn.bigdata.artist.mr.AristJoinAristUser1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/15 13:40
 */
@SuppressWarnings("all")
public class AristJoinAristUserMapper extends Mapper<Object, Text, Text, ArtistCombineValues> {
    private static final Logger logger = LoggerFactory.getLogger(AristJoinAristUserMapper.class);

    private ArtistCombineValues combineValues = new ArtistCombineValues();
    private Text flag = new Text();
    private Text joinKey = new Text();
    private Text secondPart = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String line = value.toString();
        if (StringUtils.isEmpty(line)) return;
        if (filePath.endsWith("artist_data_1.txt")) {
            logger.info("文件路径是：" + filePath);
            String[] split = line.split("\t");
            if (split.length != 2) return;
            joinKey.set(split[0]);
            secondPart.set(split[1]);
            flag.set("0");
            combineValues.setJoinKey(joinKey);
            combineValues.setSecondPart(secondPart);
            combineValues.setFlag(flag);
            context.write(combineValues.getJoinKey(), combineValues);
        } else if (filePath.endsWith("user_artist_data.txt")) {
            String[] split = line.split("\t");
            if (split.length != 3) return;
            joinKey.set(split[1]);
            secondPart.set(split[0] + "\t" + split[2]);
            flag.set("1");
            combineValues.setJoinKey(joinKey);
            combineValues.setSecondPart(secondPart);
            combineValues.setFlag(flag);
            context.write(combineValues.getJoinKey(), combineValues);
        }
    }
}
