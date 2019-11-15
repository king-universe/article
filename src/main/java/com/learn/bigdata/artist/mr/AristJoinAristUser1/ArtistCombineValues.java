package com.learn.bigdata.artist.mr.AristJoinAristUser1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author 王犇
 * @version 1.0
 * @date 2019/11/15 13:15
 */
public class ArtistCombineValues implements WritableComparable<ArtistCombineValues> {
    private Text joinKey;//链接关键字
    private Text flag;//文件来源标志
    private Text secondPart;//除了链接键外的其他部分

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag = flag;
    }

    public Text getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(Text secondPart) {
        this.secondPart = secondPart;
    }


    public ArtistCombineValues() {
        this.joinKey = new Text();
        this.flag = new Text();
        this.secondPart = new Text();
    }


    @Override
    public int compareTo(ArtistCombineValues o) {
        return this.joinKey.compareTo(o.getJoinKey());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.joinKey.write(dataOutput);
        this.flag.write(dataOutput);
        this.secondPart.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.joinKey.readFields(dataInput);
        this.flag.readFields(dataInput);
        this.secondPart.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "ArtistCombineValues{" +
                "joinKey=" + joinKey +
                ", flag=" + flag +
                ", secondPart=" + secondPart +
                '}';
    }
}
