package com.smn.hadoop.gzip;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {

	private String pageTitle;
	private int hitCount;

	public CompositeKey() {

	}

	public CompositeKey(String pageTitle, int hitCount) {
		super();
		this.pageTitle = pageTitle;
		this.hitCount = hitCount;
	}

	@Override
	public String toString() {
		return "CompositeKey [pageTitle=" + pageTitle + ", hitCount="
				+ hitCount + "]";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageTitle = WritableUtils.readString(in);
		hitCount = WritableUtils.readVInt(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, pageTitle);
		WritableUtils.writeVInt(out, hitCount);

	}

	public String getPageTitle() {
		return pageTitle;
	}

	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}

	public int getHitCount() {
		return hitCount;
	}

	public void setHitCount(int hitCount) {
		this.hitCount = hitCount;
	}

	@Override
	public int compareTo(CompositeKey compositeKey) {

		int result = -1
				* new Integer(hitCount).compareTo(compositeKey.getHitCount());
		if (0 == result) {
			result = pageTitle.compareTo(compositeKey.getPageTitle());
		}
		return result;
	}

}
