package com.zbdx.shuffle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserRunJob {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Path inputPath=new Path("hdfs://192.168.210.128:9000/opt/data/data");
		Path outputPath=new Path("hdfs://192.168.210.128:9000/opt/test/user_out");
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "192.168.210.128:9001");
		
		
		Job job=new Job(conf);
		job.setJarByClass(UserRunJob.class);
		job.setJobName("shuffle-user-hqh");
		job.setMapperClass(UserMap.class);
		job.setReducerClass(UserReduce.class);
		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
