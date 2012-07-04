package pl.stupaq.hadoop.prefixes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class RangeWritable implements WritableComparable<RangeWritable> {

	private LongWritable first;
	private LongWritable last;

	public RangeWritable() {
		this(0, 0);
	}

	public RangeWritable(long first, long last) {
		this(new LongWritable(first), new LongWritable(last));
	}

	public RangeWritable(LongWritable first, LongWritable last) {
		super();
		this.first = first;
		this.last = last;
	}

	public LongWritable getFirst() {
		return first;
	}

	public LongWritable getLast() {
		return last;
	}

	public boolean isNextRight(RangeWritable range) {
		return last.get() + 1 == range.first.get();
	}

	public boolean rightMeld(RangeWritable range) {
		if (isNextRight(range)) {
			last.set(range.last.get());
			return true;
		}
		return false;
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
		return first.hashCode() * 163 + last.hashCode() * 163;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RangeWritable) {
			RangeWritable tp = (RangeWritable) o;
			return first.equals(tp.first) && last.equals(tp.last);
		}
		return false;
	}

	@Override
	public String toString() {
		return first + " " + last;
	}

	@Override
	public int compareTo(RangeWritable o) {
		int cmp = first.compareTo(o.first);
		if (cmp == 0)
			cmp = last.compareTo(o.last);

		return cmp;
	}
}
