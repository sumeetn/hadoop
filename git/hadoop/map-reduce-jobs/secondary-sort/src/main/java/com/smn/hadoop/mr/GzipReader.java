package com.smn.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.smn.hadoop.helper.CompositeKey;
import com.smn.hadoop.helper.CompositeKeyComparator;
import com.smn.hadoop.helper.CustomKeyPartitioner;
import com.smn.hadoop.helper.GroupingComparator;

public class GzipReader extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration config = getConf();
		Job job = new Job(config, "Gzip Reader");
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setPartitionerClass(CustomKeyPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setMapperClass(GzipMapper.class);
		job.setReducerClass(GzipReducer.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("secondary-sort.jar /input /output");
			System.exit(0);
		}

		int exitCode = ToolRunner.run(new GzipReader(), args);
		System.exit(exitCode);

	}

}
