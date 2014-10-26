package com.smn.hadoop.spocosy.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ObjectKeyReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			multipleOutputs.write(NullWritable.get(), value, key.toString());
		}
	}

	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}

}
