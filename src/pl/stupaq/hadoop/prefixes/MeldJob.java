package pl.stupaq.hadoop.prefixes;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MeldJob extends Job {

	private static final String MONOID_CONF = "WritableMonoidClass";

	private static class MeldMapper extends
			Mapper<Text, Text, RangeWritable, WritableMonoid> {

		WritableMonoid monoid;
		RangeWritable range;
		WritableMonoid sum;

		@Override
		protected void setup(
				Mapper<Text, Text, RangeWritable, WritableMonoid>.Context context)
				throws IOException, InterruptedException {
			// figure out monoid to use
			try {
				Configuration conf = context.getConfiguration();
				String className = conf.get(MONOID_CONF);
				monoid = (WritableMonoid) Class.forName(className)
						.newInstance();
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException();
			}

			range = new RangeWritable(Long.MIN_VALUE, Long.MIN_VALUE);
			sum = monoid.neutral();
		};

		@Override
		protected void map(
				Text key,
				Text value,
				Mapper<Text, Text, RangeWritable, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			// parse input
			Scanner scanner = new Scanner(key.toString());
			RangeWritable new_range = new RangeWritable(scanner.nextLong(),
					scanner.nextLong());
			WritableMonoid num = monoid.neutral();
			num.fromText(value);

			// accumulate
			if (range.rightMeld(new_range)) {
				// join with last range
				sum.rightOpMutable(num);
			} else {
				if (range.getLast().get() >= 0) {
					// emit range
					context.write(range, sum);
				}
				// save new range
				range = new_range;
				sum = num;
			}
		};

		@Override
		protected void cleanup(
				Mapper<Text, Text, RangeWritable, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			if (range.getLast().get() >= 0) {
				// emit last range
				context.write(range, sum);
			}
		};
	}

	public MeldJob(Configuration conf, Class<?> monoid, int level)
			throws IOException {
		super(conf);

		getConfiguration().set(MONOID_CONF, monoid.getName());

		setJarByClass(MeldJob.class);
		setJobName(MeldJob.class.getName() + "@" + level);

		setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(this, new Path(
				"/user/user/input/prefixes/" + level));

		setMapperClass(MeldMapper.class);

		setMapOutputKeyClass(RangeWritable.class);
		setMapOutputValueClass(monoid);

		setOutputKeyClass(RangeWritable.class);
		setOutputValueClass(monoid);

		setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(this, new Path(
				"/user/user/input/prefixes/" + (level + 1)));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("/user/user/input/prefixes/" + (level + 1)), true);
	}

	public MeldJob(Class<?> monoid, int level) throws IOException {
		this(new Configuration(), monoid, level);
	}
}
