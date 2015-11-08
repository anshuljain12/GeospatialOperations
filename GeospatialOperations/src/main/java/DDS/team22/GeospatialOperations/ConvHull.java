package DDS.team22.GeospatialOperations;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ConvHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("GeoConvexHull").setMaster("spark://192.168.0.6:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Helper.ConvexHull(sc,"ConvexHullTestData.csv");
		sc.close();
	}
	
	
	
}
