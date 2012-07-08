package pl.stupaq.hadoop.prefixes;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

public class KeyValueRangeMonoidInputFormat extends
		FileInputFormat<RangeWritable, WritableMonoid> {

	private static String argMonoidName = "pl.stupaq.hadoop.prefixes.monoi.class";

	public static String getArgMonoidName() {
		return argMonoidName;
	}

	public static void setArgMonoidName(String argMonoidName) {
		KeyValueRangeMonoidInputFormat.argMonoidName = argMonoidName;
	}

	private class RangeMonoidRecordReader extends
			RecordReader<RangeWritable, WritableMonoid> {

		private KeyValueLineRecordReader reader;
		private final WritableMonoid monoid;

		public RangeMonoidRecordReader(TaskAttemptContext context)
				throws IOException {

			Configuration conf = context.getConfiguration();
			monoid = WritableMonoidUtils.getInstance(conf.get(argMonoidName));
			reader = new KeyValueLineRecordReader(conf);
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public RangeWritable getCurrentKey() throws IOException,
				InterruptedException {

			String sKey = reader.getCurrentKey().toString();
			Scanner scanner = new Scanner(sKey);
			return new RangeWritable(scanner.nextLong(), scanner.nextLong());
		}

		@Override
		public WritableMonoid getCurrentValue() throws IOException,
				InterruptedException {

			Text tValue = reader.getCurrentValue();
			WritableMonoid value = monoid.neutral();
			value.fromText(tValue);
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return reader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			reader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reader.nextKeyValue();
		}
	}

	@Override
	public RecordReader<RangeWritable, WritableMonoid> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {

		return new RangeMonoidRecordReader(context);
	}
}
