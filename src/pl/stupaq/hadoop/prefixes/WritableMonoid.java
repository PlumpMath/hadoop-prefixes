package pl.stupaq.hadoop.prefixes;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public interface WritableMonoid extends Writable, Cloneable {

	void fromText(Text value);

	WritableMonoid neutral();

	WritableMonoid rightOp(WritableMonoid right);

	void rightOpMutable(WritableMonoid right);

	WritableMonoid clone();
}
