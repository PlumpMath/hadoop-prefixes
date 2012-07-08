package pl.stupaq.hadoop.prefixes;

import java.io.IOException;

public class WritableMonoidUtils {

	private WritableMonoidUtils() {
	}

	public static WritableMonoid getInstance(String className)
			throws IOException {
		try {
			return (WritableMonoid) Class.forName(className).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			throw new IOException();
		}
	}
}
