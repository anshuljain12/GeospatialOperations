package DDS.team22.GeospatialOperations;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ConvHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("GeoConvexHull");
		JavaSparkContext sc = new JavaSparkContext(conf);
		if (args.length<2){
			System.out.println("Insufficient number of inputs");
			sc.close();
			return;
		}
		//List<String> result = Helper.ConvexHull(sc,args[0]);
		//JavaRDD<String> global_result = sc.parallelize(result).repartition(1);
		JavaRDD<String> global_result = Helper.ConvexHull(sc,args[0]);
		global_result.repartition(1);
		String output_folder = args[1]+Utils.getCurrentTime();
		global_result.distinct().saveAsTextFile(output_folder);
		sc.close();
	}
}
