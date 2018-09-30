package com.tf.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UidMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key,
                       Text value,
                       org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,Text,Text>.Context context)
                       throws IOException, InterruptedException {
        String line = value.toString();
        String[] ss = line.split("\t",-1);
        if(ss !=null && ss.length==6){
            String keyword = ss[2];
            if (keyword.indexOf("仙剑奇侠传")>=0){
                context.write(new Text(ss[1]),new Text(ss[2]));
            }
        }
    }
}
