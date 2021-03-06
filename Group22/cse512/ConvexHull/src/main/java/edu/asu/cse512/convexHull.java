package edu.asu.cse512;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class convexHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("Group22-GeoConvexHull");
		JavaSparkContext sc = new JavaSparkContext(conf);
		if (args.length < 2) {
			System.out.println("Insufficient number of inputs");
			sc.close();
			return;
		}

		String output_folder = args[1];
		JavaRDD<String> global_result = Helper.ConvexHull(sc, args[0]);
		JavaRDD<PointDouble> double_points = global_result.distinct().map(PointDouble.ToPointDouble);// makings sure we have distinct points
		JavaRDD<PointDouble> all_double_points = double_points.mapPartitions(PointDouble.SortRDD);// sorting the output
		global_result = all_double_points.map(PointDouble.PointToString);// converting JavaRDD object to JavaRDD string.
		global_result.saveAsTextFile(output_folder); // saving the output to a text file.
		sc.close();
	}
}