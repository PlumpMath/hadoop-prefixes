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

		MeldJob job = new MeldJob(IntWritableMonoid.class, 0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}