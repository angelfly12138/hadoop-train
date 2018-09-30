package com.tf.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class XjTest {
    public static void main(String[] args) throws Exception{

        String inpath = args[0];
        String outpath = args[1];

        Job job = Job.getInstance();
        job.setJobName(XjTest.class.getName());
        job.setJarByClass(XjTest.class);
        job.setMapperClass(UidMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(inpath));
        FileOutputFormat.setOutputPath(job,new Path(outpath));

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
