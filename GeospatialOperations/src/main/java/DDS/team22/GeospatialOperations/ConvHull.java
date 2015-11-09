package DDS.team22.GeospatialOperations;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ConvHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("GeoConvexHull");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> result = Helper.ConvexHull(sc,"input_data/ConvexHullTestData.csv");
		JavaRDD<String> global_result = sc.parallelize(result).repartition(1);
		String output_folder = "output_data/convexHullResult_"+Utils.getCurrentTime();
		global_result.saveAsTextFile(output_folder);
		sc.close();
	}
}
