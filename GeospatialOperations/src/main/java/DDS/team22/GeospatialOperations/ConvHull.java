package DDS.team22.GeospatialOperations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ConvHull {

	public static void main(String[] args) throws IOException {
		SparkConf conf = new SparkConf().setAppName("GeoConvexHull").setMaster("spark://192.168.0.6:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String result = Helper.ConvexHull(sc,"input_data/ConvexHullTestData.csv");
		File file=new File("output_data/convexHullResult_"+Utils.getCurrentTime()+".txt");
		FileWriter fw;
		if(file.exists())
			fw=new FileWriter(file);
		else{
			file.createNewFile();
			fw=new FileWriter(file);
		}
		fw.write(result);
		fw.close();
		sc.close();
	}
}
