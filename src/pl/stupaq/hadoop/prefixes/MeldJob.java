package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

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

			// accumulate
			if (range.meldNextRange(key)) {
				// join with last range
				sum.rightOpMutable(value);
			} else {
				if (range.getLast() >= 0) {
					// emit range
					context.write(range, sum);
				}
				// save new range
				range = key.clone();
				sum = value.clone();
			}
		};

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			if (range.getLast() >= 0) {
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
		setJarByClass(SplitJob.class);
		setJobName(SplitJob.class.getName() + "@" + level);

		// setup custom input format
		if (level == 0) {
			KeyValueRangeMonoidInputFormat.setArgMonoidName(ARG_MONOID_NAME);
			setInputFormatClass(KeyValueRangeMonoidInputFormat.class);
		} else {
			setInputFormatClass(SequenceFileInputFormat.class);
		}
		FileInputFormat.addInputPath(this, inputPath);

		setMapperClass(MeldMapper.class);
		setMapOutputKeyClass(RangeWritable.class);
		setMapOutputValueClass(monoidClass);

		setReducerClass(MeldReducer.class);
		setOutputKeyClass(RangeWritable.class);
		setOutputValueClass(monoidClass);

		setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(this, outputPath);

		// prepare file system destination
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);
		fs.close();

		// set total order partitioner
		setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(conf, partitionPath);
		// prepare partitions file
		InputSampler.writePartitionFile(this,
				new InputSampler.SplitSampler<RangeWritable, WritableMonoid>(
						getNumReduceTasks()));
	}

	public MeldJob(Class<?> monoid, int level) throws IOException,
			ClassNotFoundException, InterruptedException {
		this(new Configuration(), monoid, level);
	}
}