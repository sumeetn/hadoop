package com.smn.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.smn.hadoop.helper.CompositeKey;

public class GzipMapper extends
		Mapper<LongWritable, Text, CompositeKey, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		String[] attributes = line.split(" ");
		String title = "null";
		int count = 0;
		try {
			title = attributes[1];
			count = Integer.parseInt(attributes[2]);
			context.write(new CompositeKey(title, count),new IntWritable(count));
			//context.write(new CompositeKey(title, count),new Text(title));
			
		} catch (ArrayIndexOutOfBoundsException e) {
			System.out.println("failed for " + line);
		}

	}
}
