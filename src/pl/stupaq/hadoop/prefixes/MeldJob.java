package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

// TODO automatically set reduce tasks number
// TODO manage input and output formats
public class MeldJob extends Job {

	private static final String PARTITION_NAME = "_partition.lst";
	private static final String INPUT_LEVELS_PATH = "/user/user/input/prefixes/";

	private static final String ARG_MONOID_NAME = "pl.stupaq.hadoop.prefixes.monoi.class";

	private static class MeldMapper
			extends
			Mapper<RangeWritable, WritableMonoid, RangeWritable, WritableMonoid> {

		RangeWritable range;
		WritableMonoid sum;

		@Override
		protected void setup(
				Mapper<RangeWritable, WritableMonoid, RangeWritable, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			range = new RangeWritable(Long.MIN_VALUE, Long.MIN_VALUE);
			sum = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
		};

		@Override
		protected void map(RangeWritable key, WritableMonoid value,
				Context context) throws IOException, InterruptedException {

			// parse input
			RangeWritable new_range = key;
			WritableMonoid num = value;

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
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			if (range.getLast().get() >= 0) {
				// emit last range
				context.write(range, sum);
			}
		};
	}

	private static class MeldReducer
			extends
			Reducer<RangeWritable, WritableMonoid, RangeWritable, WritableMonoid> {

		private WritableMonoid monoid;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			monoid = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
		};

		@Override
		protected void reduce(RangeWritable key,
				Iterable<WritableMonoid> values, Context context)
				throws IOException, InterruptedException {

			WritableMonoid sum = monoid.neutral();

			for (WritableMonoid value : values) {
				sum.rightOpMutable(value);
			}
			context.write(key, sum);
		};
	}

	public MeldJob(Configuration configuration, Class<?> monoidClass, int level)
			throws IOException, ClassNotFoundException, InterruptedException {
		super(configuration);
		configuration = null;

		conf.set(ARG_MONOID_NAME, monoidClass.getName());

		Path inputPath = new Path(INPUT_LEVELS_PATH + level);
		Path outputPath = new Path(INPUT_LEVELS_PATH + (level + 1));
		Path partitionPath = new Path(INPUT_LEVELS_PATH + PARTITION_NAME);

		// setup job
		setJarByClass(MeldJob.class);
		setJobName(MeldJob.class.getName() + "@" + level);

		// setup custom input format
		KeyValueRangeMonoidInputFormat.setArgMonoidName(ARG_MONOID_NAME);
		setInputFormatClass(KeyValueRangeMonoidInputFormat.class);
		FileInputFormat.addInputPath(this, inputPath);

		setMapperClass(MeldMapper.class);

		setMapOutputKeyClass(RangeWritable.class);
		setMapOutputValueClass(monoidClass);

		setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(conf, partitionPath);

		setReducerClass(MeldReducer.class);

		setOutputKeyClass(RangeWritable.class);
		setOutputValueClass(monoidClass);

		setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(this, outputPath);

		// prepare partitions file
		InputSampler.writePartitionFile(this,
				new InputSampler.SplitSampler<RangeWritable, WritableMonoid>(
						getNumReduceTasks()));

		// prepare file system destination
		FileSystem.get(conf).delete(outputPath, true);
	}

	public MeldJob(Class<?> monoid, int level) throws IOException,
			ClassNotFoundException, InterruptedException {
		this(new Configuration(), monoid, level);
	}
}
