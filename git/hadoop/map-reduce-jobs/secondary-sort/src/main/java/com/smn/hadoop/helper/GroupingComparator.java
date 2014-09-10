package com.smn.hadoop.helper;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

	protected GroupingComparator() {
		super(CompositeKey.class, true);

	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;
		return key1.getPageTitle().compareTo(key2.getPageTitle());
	}
}
