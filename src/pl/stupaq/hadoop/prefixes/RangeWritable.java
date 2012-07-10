package pl.stupaq.hadoop.prefixes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// NOTE magic happens in comparator
public class RangeWritable implements WritableComparable<RangeWritable>,
		Cloneable {

	private static final long LOWEST_INDEX = 1;

	private long first;
	private long last;

	public RangeWritable() {
		this(0, 0);
	}

	public RangeWritable(long first, long last) {
		super();
		this.first = first;
		this.last = last;
	}

	public long getFirst() {
		return first;
	}

	public long getLast() {
		return last;
	}

	public boolean isNextRange(RangeWritable range) {
		return getLast() + 1 == range.getFirst();
	}

	public boolean meldNextRange(RangeWritable range) {
		if (isNextRange(range)) {
			last = range.last;
			return true;
		}
		return false;
	}

	public boolean isPrefix() {
		return first == LOWEST_INDEX;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(first - Long.MIN_VALUE);
		out.writeLong(last - Long.MIN_VALUE);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readLong() + Long.MIN_VALUE;
		last = in.readLong() + Long.MIN_VALUE;
	}

	@Override
	public int hashCode() {
		return new Long(first).hashCode() * 313 + new Long(last).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RangeWritable) {
			RangeWritable r = (RangeWritable) o;
			return first == r.first && last == r.last;
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + last;
	}

	private RangeWritable unprefixed() {
		if (isPrefix())
			return new RangeWritable(getLast(), Long.MIN_VALUE);
		else
			return this;
	}

	private int lexicographicalCompare(RangeWritable r) {
		int cmp = Long.compare(first, r.first);
		if (cmp == 0)
			cmp = Long.compare(last, r.last);
		return cmp;
	}

	@Override
	public int compareTo(RangeWritable r) {
		return this.unprefixed().lexicographicalCompare(r.unprefixed());
	}

	@Override
	public RangeWritable clone() {
		return new RangeWritable(getFirst(), getLast());
	}
}
