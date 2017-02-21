package org.ss.mv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author SONG
 *         Intermediate keys (ib, kb, m)
 */
public class MyKey implements WritableComparable<MyKey> {

	public VLongWritable ib;
	public VLongWritable kb;
	public ByteWritable m;

	static {
		WritableComparator.define(MyKey.class, new KeyComparator());
	}

	public MyKey() {
		ib = new VLongWritable();
		kb = new VLongWritable();
		m = new ByteWritable();
	}

	public MyKey(VLongWritable ib, VLongWritable kb, ByteWritable m) {
		this.ib = ib;
		this.kb = kb;
		this.m = m;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ib.readFields(in);
		kb.readFields(in);
		m.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		ib.write(out);
		kb.write(out);
		m.write(out);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MyKey))
			return false;

		MyKey other = (MyKey) obj;
		return ib.equals(other.ib) && kb.equals(other.kb) && m.equals(other.m);
	}

	/**
	 * sort in increasing order first by ib, then by kb, then by m. Note that m
	 * = 0 for A data and m = 1 for B data.
	 */
	@Override
	public int compareTo(MyKey o) {
		int cmp = ib.compareTo(o.ib);
		if (0 != cmp)
			return cmp;

		cmp = kb.compareTo(o.kb);
		if (0 != cmp)
			return cmp;

		return m.compareTo(o.m);
	}

	@Override
	public String toString() {
		return "key=(" + ib + "," + kb + "," + m + ")";
	}

}
