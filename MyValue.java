package org.ss.mv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyValue implements Writable {
	public long index1;
	public long index2;
	public double v;

	@Override
	public void write(DataOutput out) throws IOException {
		if (null != out) {
			out.writeLong(index1);
			out.writeLong(index2);
			out.writeDouble(v);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (null != in) {
			index1 = in.readLong();
			index2 = in.readLong();
			v = in.readDouble();
		}
	}

	@Override
	public String toString() {
		return "value=(" + index1 + "," + index2 + "," + v + ")";
	}

}
