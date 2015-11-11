package DDS.team22.FarthestPair;

import java.io.IOException;
/**
 * Find the Geometry Farthest Pair of Points
 * Input: Points like x,y
 * Output: Pair of points like x1,y1 and x2,y2 in new lines
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class farthestPair {
	public static void main(String[] args) throws IOException {
		/*
		 * Input File path and output directory for the result. Call Farthest
		 * Point Function
		 */
		if (args.length < 2) {
			System.out.println("Insufficient number of inputs");
			return;
		}
		String inputFile = args[0];
		String outputDir = args[1] + Utils.getCurrentTime();
		farthestPoints(inputFile, outputDir);

	}

	public static void farthestPoints(String inputFile, String outputDir) throws IOException {
		/*
		 * Create the java SparkContext and call the function getFarthestPoints
		 * getFarthestPoints function computes the pair of points with farthest
		 * distances
		 */
		SparkConf conf = new SparkConf().setAppName("Group22-FarthestPointinSetofPoints");
		JavaSparkContext sc = new JavaSparkContext(conf);
		getFarthestPoints(inputFile, outputDir, sc);
	}

	@SuppressWarnings("serial")
	public static void getFarthestPoints(String inputFile, String outputDir, JavaSparkContext sc) throws IOException {
		/*
		 * Read the input file into RDD and transform the values into Points
		 * Take cartesian product of rdd with itself to get Pair of points Rdd
		 * Create a Pair Rdd with distance between pair of points and the pair
		 * of points
		 */

		// Read the input file and converts it into Rdd

		// List<String> cor_list = Helper.ConvexHull(sc, inputFile);
		// JavaRDD<String> inputRdd = sc.parallelize(cor_list);
		JavaRDD<String> inputRdd = Helper.ConvexHull(sc, inputFile);
		// Get the String points into objects of point class

		JavaRDD<Point> pointRdd = inputRdd.map(new Function<String, Point>() {
			public Point call(String s) {
				String[] splitstring = s.split(",");
				Float[] point = new Float[splitstring.length];
				for (int i = 0; i < splitstring.length; i++)
					point[i] = Float.parseFloat(splitstring[i]);
				return new Point(point[0], point[1]);
			}
		});

		// Create the cartesian product of the rdd to find all pair of points

		JavaPairRDD<Point, Point> cartesian = pointRdd.cartesian(pointRdd);

		/*
		 * Create the Pair Rdds with distance between pair of points with same
		 * pair of points
		 */

		JavaPairRDD<Double, PairOfPoints> resultingRdd = cartesian
				.mapToPair(new PairFunction<Tuple2<Point, Point>, Double, PairOfPoints>() {
					public Tuple2<Double, PairOfPoints> call(Tuple2<Point, Point> v1) {
						double distance;
						Point p1 = v1._1();
						Point p2 = v1._2();
						distance = Point.getDistPairOfPoints(p1, p2);
						return new Tuple2<Double, PairOfPoints>(distance, new PairOfPoints(p1, p2));

					}
				}).cache();

		// Sort the rdd by key in descending order to get maximum distance

		JavaPairRDD<Double, PairOfPoints> farthestPair = resultingRdd.sortByKey(false);

		// Get the first line of the rdd with is the farthest pair of points

		Tuple2<Double, PairOfPoints> PointFarthest = farthestPair.first();

		/*
		 * Get Rdd with all the elements of farthest distance
		 */
		double key = PointFarthest._1();
		List<PairOfPoints> pp = new ArrayList<PairOfPoints>();
		List<String> pts = new ArrayList<String>();
		pp = farthestPair.lookup(key);
		for (int i = 0; i < pp.size(); i++) {
			pts.add(Float.toString(pp.get(i).getPoint()[0].xcord()) + ","
					+ Float.toString(pp.get(i).getPoint()[0].ycord()));
			pts.add(Float.toString(pp.get(i).getPoint()[1].xcord()) + ","
					+ Float.toString(pp.get(i).getPoint()[1].ycord()));

		}
		// Parse Objects and Create a rdd with the farthest pair of points and
		// saves it into output directory
		sc.parallelize(pts).distinct().repartition(1).map(PointDouble.ToPointDouble).mapPartitions(PointDouble.SortRDD)
				.map(PointDouble.PointToString).saveAsTextFile(outputDir);

	}

}
