package org.ss.mv;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class KeyComparator extends WritableComparator {

	public KeyComparator() {
		super(MyKey.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int cmp = 0;
		try {
			long ib_1 = readVLong(b1, s1);
			long ib_2 = readVLong(b2, s2);
			cmp = ib_1 < ib_2 ? -1 : (ib_1 == ib_2) ? 0 : 1;
			if (0 != cmp)
				return cmp;

			int n2_1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2_2 = WritableUtils.decodeVIntSize(b2[s2]);
			long kb_1 = readVLong(b1, s1 + n2_1);
			long kb_2 = readVLong(b2, s2 + n2_2);
			cmp = kb_1 < kb_2 ? -1 : (kb_1 == kb_2) ? 0 : 1;
			if (0 != cmp)
				return cmp;

			int n3_1 = WritableUtils.decodeVIntSize(b1[s1 + n2_1]);
			int n3_2 = WritableUtils.decodeVIntSize(b2[s2 + n2_2]);
			int m_1 = readVInt(b1, s1 + n2_1 + n3_1);
			int m_2 = readVInt(b2, s2 + n2_2 + n3_2);
			return m_1 < m_2 ? -1 : (m_1 == m_2) ? 0 : 1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
