package com.learn.bigdata.artist.mr.AristJoinAristUser1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/15 14:00
 */
public class AristJoinAristUserReducer extends Reducer<Text, ArtistCombineValues, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(AristJoinAristUserReducer.class);
    //存储一个分组中的左表信息
    private ArrayList<Text> leftTable = new ArrayList<Text>();
    //存储一个分组中的右表信息
    private ArrayList<Text> rightTable = new ArrayList<Text>();
    private Text secondPar = null;
    private Text output = new Text();

    @Override
    protected void reduce(Text key, Iterable<ArtistCombineValues> values, Context context) throws IOException, InterruptedException {
        leftTable.clear();
        rightTable.clear();

        for (ArtistCombineValues value : values) {
            secondPar = new Text(value.getSecondPart().toString());
            if ("0".equals(value.getFlag().toString().trim())) {
                leftTable.add(secondPar);

            } else if ("1".equals(value.getFlag().toString().trim())) {
                rightTable.add(secondPar);
            }
        }
        logger.info("tb_dim_city:" + leftTable.toString());
        logger.info("tb_user_profiles:" + rightTable.toString());
        for (Text leftPart : leftTable) {
            for (Text rightPart : rightTable) {
                output.set(leftPart + "\t" + rightPart);
                context.write(key, output);
            }
        }
    }
}
