package com.smn.hadoop.helper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CustomKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {

	HashPartitioner<Text, IntWritable> hashPartitioner = new HashPartitioner<Text, IntWritable>();
	Text newKey = new Text();

	@Override
	public int getPartition(CompositeKey key, IntWritable value, int numReduceTasks) {
		try {

			newKey.set(key.getPageTitle());
			return hashPartitioner.getPartition(newKey, value, numReduceTasks);
		} catch (Exception e) {
			e.printStackTrace();
			return (int) (Math.random() * numReduceTasks);
		}
	}

}
