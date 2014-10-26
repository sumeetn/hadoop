package com.smn.hadoop.spocosy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EventIdSeparator extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = JobBuilder.parseInputAndOutput(this, conf, args);
		if (job == null) {
			return -1;
		}
		
		
		job.setMapperClass(ObjectKeyMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setReducerClass(ObjectKeyReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(
				new EventIdSeparator(), args);
		System.exit(exitCode);
	}

}
