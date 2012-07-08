package pl.stupaq.hadoop.prefixes.mapfiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import pl.stupaq.hadoop.prefixes.WritableMonoid;
import pl.stupaq.hadoop.prefixes.WritableMonoidUtils;

public class MeldJob extends Job {

	private static final String PARTITION_NAME = "_partition.lst";
	private static final String INPUT_LEVELS_PATH = "/user/user/input/prefixes/";

	private static final String ARG_MONOID_NAME = "pl.stupaq.hadoop.prefixes.monoid.class";

	private static class MeldMapper extends
			Mapper<Text, Text, Text, WritableMonoid> {

		String splitName;
		WritableMonoid sum;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			// determine monoid to use
			sum = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
			// get split name
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			splitName = fileSplit.getPath().getName();

			// sum is already initialized with neutral element
		};

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			// parse input
			WritableMonoid num = sum.neutral();
			num.fromText(value);

			// accumulate
			sum.rightOpMutable(num);
		};

		@Override
		protected void cleanup(
				Mapper<Text, Text, Text, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			context.write(new Text(splitName), sum);
		};
	}

	private static class MeldReducer extends
			Reducer<Text, WritableMonoid, Text, WritableMonoid> {

		WritableMonoid monoid;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			// determine monoid to use
			monoid = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
		}

		@Override
		protected void reduce(Text key, Iterable<WritableMonoid> values,
				Context context) throws IOException, InterruptedException {

			WritableMonoid sum = monoid.neutral();

			for (WritableMonoid value : values) {
				sum.rightOpMutable(value);
			}
			context.write(key, sum);
		};
	}

	private void writePartitionFile(Path inputPath) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		String partition = TotalOrderPartitioner.getPartitionFile(conf);
		Writer partitionWriter = SequenceFile.createWriter(fs, conf, new Path(
				partition), Text.class, NullWritable.class);

		FileStatus[] files = fs.listStatus(inputPath);
		int interval = files.length / getNumReduceTasks();
		for (int i = interval; i < files.length; i += interval) {
			FileStatus fileStatus = files[i];
			partitionWriter.append(new Text(fileStatus.getPath().getName()),
					NullWritable.get());
		}
		partitionWriter.close();
	}

	public MeldJob(Configuration configuration, Class<?> monoid, int level)
			throws IOException {
		super(configuration);
		configuration = null;

		conf.set(ARG_MONOID_NAME, monoid.getName());

		Path inputPath = new Path(INPUT_LEVELS_PATH + level);
		Path outputPath = new Path(INPUT_LEVELS_PATH + (level + 1));
		Path partitionPath = new Path(INPUT_LEVELS_PATH + PARTITION_NAME);

		// setup job
		setJarByClass(MeldJob.class);
		setJobName(MeldJob.class.getName() + "@" + level);

		setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(this, inputPath);

		setMapperClass(MeldMapper.class);

		setMapOutputKeyClass(Text.class);
		setMapOutputValueClass(monoid);

		setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(conf, partitionPath);

		setReducerClass(MeldReducer.class);

		setOutputKeyClass(Text.class);
		setOutputValueClass(monoid);

		setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(this, outputPath);

		// prepare partitions file
		writePartitionFile(inputPath);

		// prepare file system destination
		FileSystem.get(conf).delete(outputPath, true);
	}

	public MeldJob(Class<?> monoid, int level) throws IOException {
		this(new Configuration(), monoid, level);
	}
}
