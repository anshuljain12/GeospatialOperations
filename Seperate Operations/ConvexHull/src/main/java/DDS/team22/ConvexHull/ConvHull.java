package DDS.team22.ConvexHull;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ConvHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("GeoConvexHull");
		JavaSparkContext sc = new JavaSparkContext(conf);
		if (args.length < 2) {
			System.out.println("Insufficient number of inputs");
			sc.close();
			return;
		}

		String output_folder = args[1] + Utils.getCurrentTime();
		JavaRDD<String> global_result = Helper.ConvexHull(sc, args[0]);
		JavaRDD<PointDouble> double_points = global_result.distinct().map(PointDouble.ToPointDouble);
		JavaRDD<PointDouble> all_double_points = double_points.mapPartitions(PointDouble.SortRDD);
		global_result = all_double_points.map(PointDouble.PointToString);
		global_result.saveAsTextFile(output_folder);
		sc.close();
	}
}
