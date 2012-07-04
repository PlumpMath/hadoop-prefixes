package pl.stupaq.hadoop.prefixes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IntWritableMonoid extends IntWritable implements WritableMonoid {

	public IntWritableMonoid() {
		this(0);
	}

	public IntWritableMonoid(int value) {
		super(value);
	}

	public IntWritableMonoid(IntWritableMonoid value) {
		this(value.get());
	}

	@Override
	public WritableMonoid neutral() {
		return new IntWritableMonoid();
	}

	@Override
	public WritableMonoid rightOp(WritableMonoid right) {
		if (right instanceof IntWritableMonoid)
			return new IntWritableMonoid(get()
					+ ((IntWritableMonoid) right).get());
		else
			return new IntWritableMonoid(get());
	}

	@Override
	public void rightOpMutable(WritableMonoid right) {
		if (right instanceof IntWritableMonoid)
			set(get() + ((IntWritableMonoid) right).get());
	}

	@Override
	public void fromText(Text value) {
		set(Integer.parseInt(value.toString()));
	}
}
