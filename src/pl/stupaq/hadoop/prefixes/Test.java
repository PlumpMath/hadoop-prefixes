package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

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

		MeldJob job1 = new MeldJob(IntWritableMonoid.class, 0);
		job1.waitForCompletion(true);

		SplitJob job2 = new SplitJob(IntWritableMonoid.class, 1);
		job2.waitForCompletion(true);

		SplitJob job3 = new SplitJob(IntWritableMonoid.class, 0);
		job3.waitForCompletion(true);
	}
}