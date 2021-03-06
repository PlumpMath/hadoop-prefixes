package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

// FIXME ensure that partitioner don't send prefixes and subintervals to different reducers
public class SplitJob extends Job {

	private static final String PARTITION_NAME = "_partition.lst";
	private static final String INPUT_LEVELS_PATH = "/user/user/input/prefixes/";
	private static final String OUTPUT_LEVELS_PATH = "/user/user/output/prefixes/";

	private static final String ARG_MONOID_NAME = "pl.stupaq.hadoop.prefixes.monoi.class";

	private static class SplitMapper
			extends
			Mapper<RangeWritable, WritableMonoid, RangeWritable, WritableMonoid> {

		@Override
		protected void map(RangeWritable key, WritableMonoid value,
				Context context) throws IOException, InterruptedException {

			context.write(key, value);
		};
	}

	private static class SplitReducer
			extends
			Reducer<RangeWritable, WritableMonoid, RangeWritable, WritableMonoid> {

		private WritableMonoid sum;
		private RangeWritable last_range;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			sum = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
		};

		@Override
		protected void reduce(RangeWritable key,
				Iterable<WritableMonoid> values, Context context)
				throws IOException, InterruptedException {

			WritableMonoid value = values.iterator().next();

			if (key.isPrefix()) {
				last_range = key.clone();
				sum = value.clone();
				context.write(last_range, sum);
			} else {
				// not a prefix so try to merge
				if (last_range.meldNextRange(key)) {
					sum.rightOpMutable(value);
					context.write(last_range, sum);
				}
			}
		};
	}

	public SplitJob(Configuration configuration, Class<?> monoidClass, int level)
			throws IOException, ClassNotFoundException, InterruptedException {
		super(configuration);
		configuration = null;

		conf.set(ARG_MONOID_NAME, monoidClass.getName());

		Path inputPathMeld = new Path(INPUT_LEVELS_PATH + level);
		Path inputPathSplit = new Path(OUTPUT_LEVELS_PATH + (level + 1));
		Path outputPath = new Path(OUTPUT_LEVELS_PATH + level);
		Path partitionPath = new Path(OUTPUT_LEVELS_PATH + PARTITION_NAME);

		// setup job
		setJarByClass(SplitJob.class);
		setJobName(SplitJob.class.getName() + "@" + level);

		setMapperClass(SplitMapper.class);
		setMapOutputKeyClass(RangeWritable.class);
		setMapOutputValueClass(monoidClass);

		setReducerClass(SplitReducer.class);
		setOutputKeyClass(RangeWritable.class);
		setOutputValueClass(monoidClass);

		if (level == 0) {
			setOutputFormatClass(TextOutputFormat.class);
		} else {
			setOutputFormatClass(SequenceFileOutputFormat.class);
		}
		FileOutputFormat.setOutputPath(this, outputPath);

		// prepare file system destination and ensure input
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);

		if (fs.exists(inputPathSplit)) {
			// set total ordering
			setPartitionerClass(TotalOrderPartitioner.class);
			TotalOrderPartitioner.setPartitionFile(conf, partitionPath);
			// add input path for sampling
			MultipleInputs.addInputPath(this, inputPathSplit,
					SequenceFileInputFormat.class);
			// prepare partitions file
			InputSampler
					.writePartitionFile(
							this,
							new InputSampler.SplitSampler<RangeWritable, WritableMonoid>(
									getNumReduceTasks()));
		} else {
			// set hash partitioner
			setPartitionerClass(HashPartitioner.class);
			// set one reducer
			setNumReduceTasks(1);
		}

		// add other input paths
		if (level == 0) {
			KeyValueRangeMonoidInputFormat.setArgMonoidName(ARG_MONOID_NAME);
			MultipleInputs.addInputPath(this, inputPathMeld,
					KeyValueRangeMonoidInputFormat.class);
		} else {
			MultipleInputs.addInputPath(this, inputPathMeld,
					SequenceFileInputFormat.class);
		}

		fs.close();
	}

	public SplitJob(Class<?> monoid, int level) throws IOException,
			ClassNotFoundException, InterruptedException {
		this(new Configuration(), monoid, level);
	}
}