package edu.asu.cse512;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Aggregation {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.out.println("Insufficient number of inputs");
			return;
		}
		String inp1 = args[0]; // Input 1: csv file containing input points or
								// rectangles
		String inp2 = args[1]; // Input 2: csv file containing query input
		String out = args[2]; // Output: File where the
								// result is stored

		SparkConf conf = new SparkConf()
				.setAppName("Group22-Spacial Aggregation-");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> outputJoinQuery = null;
		try {
			outputJoinQuery = Join.spatialJoinQuery(inp1, inp2, out, "point",
					sc);
			JavaRDD<String> result = outputJoinQuery
					.map(new Function<String, String>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						public String call(String s) throws Exception {
							String[] itemArr = s.split(",");
							String id = s.substring(0, s.indexOf(',')).trim();
							return new String("<" + id + ","
									+ (itemArr.length - 1) + ">");
						}
					});

			result.repartition(1).saveAsTextFile(out);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
