package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class Test {

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// FIXME set reduce task number for each job

		Class<?> monoidClass = IntWritableMonoid.class;

		int level = 0;
		boolean success = false;
		Job job;

		do {
			job = new MeldJob(monoidClass, level);
			success = job.waitForCompletion(true);
			++level;
		} while (success && job.getNumReduceTasks() > 1);

		while (success && level >= 0) {
			job = new SplitJob(monoidClass, level);
			success = job.waitForCompletion(true);
			--level;
		}
	}
}