package com.smn.hadoop.rin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class JobBuilder {

	public static Job parseInputAndOutput(Tool tool, Configuration conf,
			String[] args) throws IOException {
		if (args.length != 2) {
			printUsage(tool, "<input> <output>");
			return null;
		}
		if (conf.get("file.pattern") == null)
			conf.set("file.pattern", "1.*.xml");
		Job job = new Job(conf);
		job.setJarByClass(tool.getClass());
		System.out.println("input "+args[0]);
		System.out.println("output "+args[1]);
		FileInputFormat.setInputPathFilter(job, FilePathFilter.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job;
	}

	public static void printUsage(Tool tool, String extraArgsUsage) {
		System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass()
				.getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}

}
