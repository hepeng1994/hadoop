package com.cattle34.study.day03.movie;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieBeen implements WritableComparable <MovieBeen>{
    //{"movie":"1287","rate":"5","timeStamp":"978302039","uid":"1"}
    private String movie;
    private int rate;
    private long timeStamp;
    private String uid;

    @Override
    public String toString() {
        return "MovieBeen{" +
                "movie='" + movie + '\'' +
                ", rate=" + rate +
                ", timeStamp='" + timeStamp + '\'' +
                ", uid='" + uid + '\'' +
                '}';
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public MovieBeen() {
    }

    public MovieBeen(String movie, int rate, long timeStamp, String uid) {
        this.movie = movie;
        this.rate = rate;
        this.timeStamp = timeStamp;
        this.uid = uid;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.movie);
        out.writeInt(this.rate);
        out.writeLong(this.timeStamp);
        out.writeUTF(this.uid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.movie = in.readUTF();
        this.rate= in.readInt();
        this.timeStamp = in.readLong();
        this.uid = in.readUTF();
    }

    @Override
    public int compareTo(MovieBeen o) {
        return o.movie.compareTo(this.movie)==0?(o.rate-this.rate):this.movie.compareTo(o.movie);
    }
}
