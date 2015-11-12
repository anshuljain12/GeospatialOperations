package edu.asu.cse512;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RangeQuery {

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Insufficient input arguments.");
			return;
		}

		String input1 = args[0];
		String input2 = args[1];
		String output = args[2];

		SpatialRangeQuery(input1, input2, output);

	}

	/**
	 * Wrapper method to find the points which are inside the rectangle
	 * 
	 * @param InputLocation1
	 *            : file location to the input points
	 * @param InputLocation2
	 *            : file location for input query window
	 * @param OutputLocation
	 *            : file location to to store the output
	 * @return
	 */
	public static boolean SpatialRangeQuery(String InputLocation1,
			String InputLocation2, String OutputLocation) {

		SparkConf sparkConfiguration = new SparkConf()
				.setAppName("Group22-RangeQuery");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
		boolean result = getRangeQuery(InputLocation1, InputLocation2,
				OutputLocation, sparkContext);
		sparkContext.close();
		return result;

	}

	/**
	 * Helper method to find the points which are inside the rectangle
	 * 
	 * @param pointsFileLocation
	 *            : file location to the input points
	 * @param queryRectangleLocation
	 *            : file location for input query window
	 * @param outputFilelocation
	 *            : file location to to store the output
	 * @param sc
	 *            : Spark context
	 * @return
	 */
	public static boolean getRangeQuery(String pointsFileLocation,
			String queryRectangleLocation, String outputFilelocation,
			JavaSparkContext sc) {

		JavaRDD<String> inputPoints = sc.textFile(pointsFileLocation);
		JavaRDD<String> queryWindow = sc.textFile(queryRectangleLocation);

		/*
		 * Creating the RDD for the input points
		 */
		JavaRDD<SpatialPoint> inputPointsRDD = inputPoints
				.map(new Function<String, SpatialPoint>() {

					private static final long serialVersionUID = 1L;

					public SpatialPoint call(String s) {
						Float[] fnum = Utils.splitingStringToFloat(s, ",");
						return new SpatialPoint(fnum[0], fnum[1], fnum[2]);
					}
				});

		/*
		 * creating RDD for query rectangle
		 */
		JavaRDD<Rectangle> queryRectangleRDD = queryWindow
				.map(new Function<String, Rectangle>() {

					private static final long serialVersionUID = 1L;

					public Rectangle call(String s) {
						Float[] fnum = Utils.splitingStringToFloat(s, ",");
						return new Rectangle(fnum[0], fnum[1], fnum[2], fnum[3]);
					}
				});

		List<Rectangle> queryRectangleList = queryRectangleRDD.collect();
		final Rectangle queryRectangle = queryRectangleList.get(0);

		/*
		 * Creating resultant RDD and adding the points which are inside the
		 * rectangle
		 */
		JavaRDD<Long> resultPointRDD = inputPointsRDD
				.map(new Function<SpatialPoint, Long>() {

					private static final long serialVersionUID = 1L;

					public Long call(SpatialPoint s) {

						if (queryRectangle.findIfPointIsInside(s)) {
							float f = s.getId();
							long l = Math.round(f);
							return l;
						} else
							return -1L;

					}
				});

		/*
		 * filtering the "resultPointRDD" to get all the points inside the query
		 * rectangle
		 */

		JavaRDD<Long> filteredResultPointRDD = resultPointRDD
				.filter(new Function<Long, Boolean>() {

					private static final long serialVersionUID = 1L;

					public Boolean call(Long l) {

						if (l != -1L) {
							return true;
						} else {
							return false;
						}
					}
				});

		/*
		 * sorting the "filteredResultPointRDD" and saving it to a text file
		 */
		filteredResultPointRDD.sortBy(new Function<Long, Long>() {

			private static final long serialVersionUID = 1L;

			public Long call(Long s) {
				return s;
			}
		}, true, 1).repartition(1).saveAsTextFile(outputFilelocation);

		return true;
	}

}