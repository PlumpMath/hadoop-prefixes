package pl.stupaq.hadoop.prefixes.mapfiles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import pl.stupaq.hadoop.prefixes.WritableMonoid;
import pl.stupaq.hadoop.prefixes.WritableMonoidUtils;

// FIXME total ordering partitioner
public class SplitJob extends Job {

	private static final String UPPERSUMS_NAME = "uppersums";
	private static final String INPUT_LEVELS_PATH = "/user/user/input/prefixes/";
	private static final String OUTPUT_LEVELS_PATH = "/user/user/output/prefixes/";

	private static final String ARG_MONOID_NAME = "pl.stupaq.hadoop.prefixes.monoid.class";
	private static final String ARG_LEVEL = "pl.stupaq.hadoop.prefixes.level";

	private static int getLevel(Configuration conf) {
		return conf.getInt(ARG_LEVEL, 0);
	}

	private static String getMapFilePath(Configuration conf, int level) {
		return OUTPUT_LEVELS_PATH + level + "/" + UPPERSUMS_NAME;
	}

	private static class SplitMapper extends
			Mapper<Text, Text, Text, WritableMonoid> {

		WritableMonoid sum;

		@Override
		protected void setup(
				Mapper<Text, Text, Text, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			// determine monoid to use
			sum = WritableMonoidUtils.getInstance(conf.get(ARG_MONOID_NAME));
			// initialized sum with value from map file
			FileSystem fs = FileSystem.get(conf);
			MapFile.Reader reader = null;
			try {
				reader = new MapFile.Reader(fs, getMapFilePath(conf,
						getLevel(conf)), context.getConfiguration());
				FileSplit split = (FileSplit) context.getInputSplit();
				String splitName = split.getPath().getName();
				reader.get(new Text(splitName), sum);
			} catch (IOException e) {
				// apparently there's no map (top level)
				// sum is already initialized with zero
			} finally {
				IOUtils.closeStream(reader);
			}
		};

		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, WritableMonoid>.Context context)
				throws IOException, InterruptedException {

			context.write(key, sum);

			WritableMonoid num = sum.neutral();
			num.fromText(value);

			sum.rightOpMutable(num);
		};
	}

	private static class SplitReducer extends
			Reducer<Text, WritableMonoid, Text, WritableMonoid> {

		private MapFile.Writer writer;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);

			try {
				String className = conf.get(ARG_MONOID_NAME);
				writer = new MapFile.Writer(conf, fs, getMapFilePath(conf,
						getLevel(conf) - 1), Text.class,
						Class.forName(className));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException();
			}
		};

		@Override
		protected void reduce(Text key, Iterable<WritableMonoid> values,
				Context context) throws IOException, InterruptedException {

			WritableMonoid value = values.iterator().next();
			writer.append(key, value);
		};

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			writer.close();
		};
	}

	private static class SplitReducerGroundLevel extends SplitReducer {
		// XXX missing
	}

	public SplitJob(Configuration conf, Class<?> monoid, int level)
			throws IOException {
		super(conf);
		conf = getConfiguration();

		getConfiguration().set(ARG_MONOID_NAME, monoid.getName());
		getConfiguration().setInt(ARG_LEVEL, level);

		setJarByClass(SplitJob.class);
		setJobName(SplitJob.class.getName() + "@" + level);

		Path inputPath = new Path(INPUT_LEVELS_PATH + level);
		Path outputPath = new Path(OUTPUT_LEVELS_PATH + (level - 1));

		setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(this, inputPath);

		setMapOutputKeyClass(Text.class);
		setMapOutputValueClass(monoid);

		setOutputKeyClass(Text.class);
		setOutputValueClass(monoid);

		if (level == 1) {
			setReducerClass(SplitReducerGroundLevel.class);
		} else {
			setMapperClass(SplitMapper.class);
			setReducerClass(SplitReducer.class);
			setNumReduceTasks(1);
		}

		setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(this, outputPath);

		// prepare file system destination
		FileSystem.get(conf).delete(outputPath, true);
	}

	public SplitJob(Class<?> monoid, int level) throws IOException {
		this(new Configuration(), monoid, level);
	}
}
