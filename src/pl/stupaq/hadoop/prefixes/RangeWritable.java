package pl.stupaq.hadoop.prefixes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

// NOTE magic happens in comparator
public class RangeWritable implements WritableComparable<RangeWritable>,
		Cloneable {

	private static final long LOWEST_INDEX = 1;

	private LongWritable first;
	private LongWritable last;

	public RangeWritable() {
		this(0, 0);
	}

	public RangeWritable(long first, long last) {
		this(new LongWritable(first), new LongWritable(last));
	}

	private RangeWritable(LongWritable first, LongWritable last) {
		super();
		this.first = first;
		this.last = last;
	}

	public long getFirst() {
		return first.get();
	}

	public long getLast() {
		return last.get();
	}

	public boolean isNextRange(RangeWritable range) {
		return last.get() + 1 == range.first.get();
	}

	public boolean meldNextRange(RangeWritable range) {
		if (isNextRange(range)) {
			last.set(range.last.get());
			return true;
		}
		return false;
	}

	public boolean isPrefix() {
		return first.get() == LOWEST_INDEX;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		last.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		last.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 313 + last.hashCode() * 163;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RangeWritable) {
			RangeWritable r = (RangeWritable) o;
			return first.equals(r.first) && last.equals(r.last);
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
		int cmp = first.compareTo(r.first);
		if (cmp == 0)
			cmp = last.compareTo(r.last);
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
