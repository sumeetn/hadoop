package com.smn.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.smn.hadoop.helper.CompositeKey;

public class GzipReducer extends
		Reducer<CompositeKey, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(CompositeKey key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();

		}
		context.write(new Text(key.getPageTitle()), new IntWritable(count));
	}
}
