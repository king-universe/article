package com.learn.bigdata.movie.mr.movieMappeJoinRatting;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/18 15:08
 */
@SuppressWarnings("all")
public class MovieJoinRattingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(com.learn.bigdata.movie.mr.movieReduceJoinRatting.MovieJoinRattingMapper.class);

    private Map<String, String> map = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split(",");
            if (split.length < 3) continue;
            String movieId = split[0];
            String movieCategory = split[split.length - 1];
            String movieName = line.substring(movieId.length() + 1, line.length() - movieCategory.length() - 1);
            map.put(movieId, movieName + "\t" + movieCategory);
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String line = value.toString();
        if (StringUtils.isEmpty(line)) return;
        if (filePath.endsWith("ratings.txt")) {
            String[] split = line.split(",");
            if (split.length < 4) return;
            String userId = split[0];
            String movieId = split[1];
            String rating = split[2];
            String timestamp = split[3];
            if (map.get(movieId) != null) {
                context.write(new Text(movieId), new Text(userId + "\t" + rating + "\t" + timestamp + "\t" + map.get(movieId)));
            } else {
                context.write(new Text(movieId), new Text(userId + "\t" + rating + "\t" + timestamp + "\tnull\tnull"));
            }
        }

    }
}

