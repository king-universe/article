package com.learn.bigdata.artist.mr.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/13 14:04
 */
public class ArtistMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    //114.92.217.149^A1450569601.351^A/what.png?u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=1450569596991&ver=1&en=e_l&pl=website&sdk=js&b_rst=1440*900&u_ud=4B16B8BB-D6AA-4118-87F8-C58680D22657&b_iev=Mozilla%2F5.0%20(Windows%20NT%205.1)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F45.0.2454.101%20Safari%2F537.36&l=zh-CN&bf_sid=33cbf257-3b11-4abd-ac70-c5fc47afb797_11177014
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("line=" + key.get() + "::::text=" + value);
        String[] strs = value.toString().split("^A");
        if (strs.length == 3) {
            context.write(NullWritable.get(), value);
        }
    }
}
