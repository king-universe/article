package com.learn.bigdata.movie.mr.movieJoinRatting;

import com.learn.bigdata.artist.mr.AristJoinAristUser1.AristJoinAristUserMapper;
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
 * @date 2019/11/15 14:27
 */
@SuppressWarnings("all")
public class MovieJoinRattingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(MovieJoinRattingMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String line = value.toString();
        if (StringUtils.isEmpty(line)) return;
        if (filePath.endsWith("movies.txt")) {
            logger.info("文件路径是：" + filePath);
            String[] split = line.split(",");
            if (split.length < 3) return;
            String movieId = split[0];
            String movieCategory = split[split.length - 1];
            String movieName = line.substring(movieId.length() + 1, line.length() - movieCategory.length() - 1);
            context.write(new Text(movieId), new Text("a:" + movieName + "\t" + movieCategory));
        } else if (filePath.endsWith("ratings.txt")) {
            String[] split = line.split(",");
            if (split.length < 4) return;
            String userId = split[0];
            String movieId = split[1];
            String rating = split[2];
            String timestamp = split[3];
            context.write(new Text(movieId), new Text("b:" + userId + "\t" + rating + "\t" + timestamp));
        }

    }
}
