package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


/**
 * Hello world!
 *
 */
public class App
{
    public static void main(String[] args) throws Exception
    {
        Path input_dir = new Path("arya/input/");
        Path output_dir = new Path("arya/output/");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job data_supermartJob = Job.getInstance(conf, "MapReduce Data Supermart");
        data_supermartJob.setJarByClass(App.class);
        data_supermartJob.setMapperClass(Uasmapper.class);
        data_supermartJob.setReducerClass(Uasreducer.class);
        data_supermartJob.setMapOutputKeyClass(Text.class);
        data_supermartJob.setMapOutputValueClass(Text.class);
        data_supermartJob.setOutputKeyClass(NullWritable.class);
        data_supermartJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(data_supermartJob, input_dir);
        FileOutputFormat.setOutputPath(data_supermartJob, output_dir);
        data_supermartJob.waitForCompletion(true);
    }
}
