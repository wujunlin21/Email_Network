package edu.gatech.cse6242;

import java.io.IOException;//
import java.util.StringTokenizer;//

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Task1 {
	

 public static class WeightMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{ 

    public void map(LongWritable offset, Text lineText, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
      String line = lineText.toString();
      String sourceNum = line.split("\t")[0];
	  String aWeight = line.split("\t")[2];
      context.write(new Text(sourceNum), new IntWritable(Integer.parseInt(aWeight)));
    }
  }

  public static class IntMaxReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce( Text sourceNum,  Iterable<IntWritable> weights,  Context context)
         throws IOException,  InterruptedException {

      int max  = 0;
      for ( IntWritable weight: weights) {
		  if (max < weight.get()) {
			  max = weight.get();
		  }
      }
      context.write(sourceNum,  new IntWritable(max));
    }
  }	
	
	
	
	

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task1");
	
	job.setJarByClass(Task1.class);//
    job.setMapperClass(WeightMapper.class);//
    job.setCombinerClass(IntMaxReducer.class);
    job.setReducerClass(IntMaxReducer.class);//
    job.setOutputKeyClass(Text.class);//
    job.setOutputValueClass(IntWritable.class);//
	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

 }

