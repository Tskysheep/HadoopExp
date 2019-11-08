package com.chap05_4.v17034460224;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Log implements WritableComparable<Log> {
    String content;
    int url_range;
    int click_range;

    public String toString(){
        return content + " " + url_range + " " + click_range;
    }

    @Override
    public int compareTo(Log log) {
        if(log.getUrl_range() == 2 && log.getClick_range() == 1) return 1;
        else return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.content);
        dataOutput.writeInt(this.url_range);
        dataOutput.writeInt(this.click_range);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.content = dataInput.readUTF();
        this.url_range = dataInput.readInt();
        this.click_range = dataInput.readInt();
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getUrl_range() {
        return url_range;
    }

    public void setUrl_range(int url_range) {
        this.url_range = url_range;
    }

    public int getClick_range() {
        return click_range;
    }

    public void setClick_range(int click_range) {
        this.click_range = click_range;
    }
}
