package com.smn.hadoop.gzip;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GzipReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(CompositeKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IntWritable value : values) {
			System.out.println(key.toString() + " ->>" + value.get());
		}
	}
}
