package edu.asu.cse512;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class Join {
	public static void main(String[] args) throws IOException {

		if (args.length < 4) {
			System.out.println("Insufficient number of inputs");
			return;
		}
		String inp1 = args[0]; // Input 1: csv file containing input points or
								// rectangles
		String inp2 = args[1]; // Input 2: csv file containing query input
		String out = args[2] + Utils.getCurrentTime(); // Output: File where the
														// result is stored
		String inputType = args[3]; // InputType: whether Point or rectangle

		SparkConf conf = new SparkConf().setAppName("Group22-Spatial Join-"
				+ inputType);
		JavaSparkContext sc = new JavaSparkContext(conf);
		spatialJoinQuery(inp1, inp2, out, inputType, sc); // Function which
															// performs spatial
															// join
															// functionality on
															// given input file.
		sc.close();
	}

	/**
	 * @param spatialObjectPathFile
	 *            : path of the input1
	 * @param qryWindowsPath
	 *            : path of input2
	 * @param outputPath
	 *            : path of output
	 * @param inputType
	 *            :Inputype = "point" or "rectangle"
	 * @param sc
	 *            : Spark context
	 * @return : Boolean
	 * @throws IOException
	 */
	public static boolean spatialJoinQuery(String spatialObjectPathFile,
			String qryWindowsPath, String outputPath, String inputType,
			JavaSparkContext sc) throws IOException {

		JavaRDD<String> queryWindow = sc.textFile(qryWindowsPath);

		// Creating RDD for query windows
		JavaRDD<Rectangle> rect1 = queryWindow
				.map(new Function<String, Rectangle>() {

					private static final long serialVersionUID = 1L;

					public Rectangle call(String s) {
						Float[] fnum = splitingStringToFloat(s, ",");
						return new Rectangle(fnum[0], fnum[1], fnum[2],
								fnum[3], fnum[4]);
					}
				});

		// if condition to check the inputType
		if (inputType.equalsIgnoreCase("point")) {
			JavaRDD<String> inputData1 = sc.textFile(spatialObjectPathFile);
			// Creating RDD for Points
			JavaRDD<SpatialPoint> pointsRDD = inputData1
					.map(new Function<String, SpatialPoint>() {
						private static final long serialVersionUID = 1L;

						public SpatialPoint call(String s) {
							Float[] fnum = splitingStringToFloat(s, ",");
							return new SpatialPoint(fnum[0], fnum[1], fnum[2]);
						}
					});
			// Broadcast PointsRDD to all nodes
			Broadcast<List<SpatialPoint>> br1 = sc.broadcast(pointsRDD
					.collect());
			final List<SpatialPoint> broad1 = br1.value();

			// RDD which contains tuples of query windows which contains list of
			// points which lies on/inside the query windows.
			JavaRDD<Tuple2<Long, List<Long>>> resultantRect1 = rect1
					.map(new Function<Rectangle, Tuple2<Long, List<Long>>>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<Long, List<Long>> call(Rectangle s) {

							List<Long> PointList = new ArrayList<Long>();
							for (SpatialPoint rec : broad1) {
								if (s.findIfPointIsInsideforPoint(rec)) {
									float f = rec.getId();
									long l = Math.round(f);
									PointList.add(l);
								}
							}
							float f = s.getId();
							long l = Math.round(f);
							return new Tuple2<Long, List<Long>>(l, PointList);
						}
					});
			// Formating the list of tuples into string in order to remove the
			// brackets.
			String formatedString1;

			List<Tuple2<Long, List<Long>>> resultantRect3 = resultantRect1
					.collect();
			List<String> strPointsList = new ArrayList<String>();
			for (Tuple2<Long, List<Long>> res : resultantRect3) {
				formatedString1 = res.toString().replace("[", "")
						.replace("]", "").replace("(", "").replace(")", "")
						.replace(" ", "");
				strPointsList.add(formatedString1);
			}
			// Saving into file.
			JavaRDD<String> finalPointResultRDD = sc.parallelize(strPointsList);
			finalPointResultRDD.sortBy(new Function<String, String>() {

				private static final long serialVersionUID = 1L;

				public String call(String s) {
					return s;
				}
			}, true, 1).repartition(1).saveAsTextFile(outputPath);

		} else if (inputType.equalsIgnoreCase("rectangle")) {

			JavaRDD<String> inputData = sc.textFile(spatialObjectPathFile);
			// Creating RDD for Rectangle
			JavaRDD<Rectangle> rect = inputData
					.map(new Function<String, Rectangle>() {

						private static final long serialVersionUID = 1L;

						public Rectangle call(String s) {
							Float[] fnum = splitingStringToFloat(s, ",");
							return new Rectangle(fnum[0], fnum[1], fnum[2],
									fnum[3], fnum[4]);
						}
					});

			// Broadcast RectangleRDD to all nodes
			Broadcast<List<Rectangle>> br = sc.broadcast(rect.collect());
			final List<Rectangle> broad = br.value();

			// RDD which contains tuples of query windows which contains list of
			// points which lies on/inside the query windows.
			JavaRDD<Tuple2<Long, List<Long>>> resultantRect = rect1
					.map(new Function<Rectangle, Tuple2<Long, List<Long>>>() {

						private static final long serialVersionUID = 1L;

						public Tuple2<Long, List<Long>> call(Rectangle s) {

							List<Long> rectangleList = new ArrayList<Long>();
							for (Rectangle rec : broad) {

								if (s.findIfPointIsInside(rec.leftLower)
										|| s.findIfPointIsInside(rec.leftUpper)
										|| s.findIfPointIsInside(rec.rightLower)
										|| s.findIfPointIsInside(rec.rightUpper)) {
									float f = rec.getId();
									long l = Math.round(f);
									rectangleList.add(l);

								} else if (s.RectangleEquals(rec)) {
									float f = rec.getId();
									long l = Math.round(f);
									rectangleList.add(l);
								}

								else if (intersection(s.leftLower,
										s.rightLower, rec.leftUpper,
										rec.leftLower)
										|| intersection(s.leftLower,
												s.rightLower, rec.rightLower,
												rec.leftLower)
										|| intersection(s.leftLower,
												s.rightLower, rec.rightUpper,
												rec.rightLower)
										|| intersection(s.leftLower,
												s.rightLower, rec.rightUpper,
												rec.leftUpper)
										|| intersection(s.leftLower,
												s.leftUpper, rec.leftUpper,
												rec.leftLower)
										|| intersection(s.leftLower,
												s.leftUpper, rec.rightLower,
												rec.leftLower)
										|| intersection(s.leftLower,
												s.leftUpper, rec.rightUpper,
												rec.rightLower)
										|| intersection(s.leftLower,
												s.leftUpper, rec.rightUpper,
												rec.leftUpper)
										|| intersection(s.rightUpper,
												s.rightLower, rec.leftUpper,
												rec.leftLower)
										|| intersection(s.rightUpper,
												s.rightLower, rec.rightLower,
												rec.leftLower)
										|| intersection(s.rightUpper,
												s.rightLower, rec.rightUpper,
												rec.rightLower)
										|| intersection(s.rightUpper,
												s.rightLower, rec.rightUpper,
												rec.leftUpper)
										|| intersection(s.rightUpper,
												s.leftUpper, rec.leftUpper,
												rec.leftLower)
										|| intersection(s.rightUpper,
												s.leftUpper, rec.rightLower,
												rec.leftLower)
										|| intersection(s.rightUpper,
												s.leftUpper, rec.rightUpper,
												rec.rightLower)
										|| intersection(s.rightUpper,
												s.leftUpper, rec.rightUpper,
												rec.leftUpper)) {
									// System.out.println("hey!!");
									float f1 = rec.getId();
									long l1 = Math.round(f1);
									rectangleList.add(l1);

								}

								if (rec.findIfPointIsInside(s.leftLower)
										&& rec.findIfPointIsInside(s.leftUpper)
										&& rec.findIfPointIsInside(s.rightLower)
										&& rec.findIfPointIsInside(s.rightUpper)) {
									// System.out.println(rec.id);
									float f = rec.getId();
									long l = Math.round(f);
									rectangleList.add(l);
								}
							}
							float f = s.getId();
							long l = Math.round(f);
							return new Tuple2<Long, List<Long>>(l,
									rectangleList);

						}
					});

			// Formating the list of tuples into string in order to remove the
			// brackets.
			String formatedString;

			List<Tuple2<Long, List<Long>>> resultantRect2 = resultantRect
					.collect();
			List<String> str = new ArrayList<String>();
			for (Tuple2<Long, List<Long>> res : resultantRect2) {
				formatedString = res.toString().replace("[", "")
						.replace("]", "").replace("(", "").replace(")", "")
						.replace(" ", "");
				str.add(formatedString);
			}
			// Saving into file.
			JavaRDD<String> finalResultRDD = sc.parallelize(str);
			finalResultRDD.sortBy(new Function<String, String>() {

				private static final long serialVersionUID = 1L;

				public String call(String s) {
					return s;
				}
			}, true, 1).repartition(1).saveAsTextFile(outputPath);
		}

		return false;

	}

	/*
	 * The algorithm for validation of Intersection is referred from this link
	 * http://stackoverflow.com/questions/25830932/how-to-find-if-two-line-
	 * segments-intersect-or-not-in-java
	 */
	public static boolean intersection(SpatialPoint x1, SpatialPoint y1,
			SpatialPoint x2, SpatialPoint y2) {
		int validate1 = validateIntersection(x1, y1, x2);
		int validate2 = validateIntersection(x1, y1, y2);
		int validate3 = validateIntersection(x2, y2, x1);
		int validate4 = validateIntersection(x2, y2, y1);

		if (validate1 != validate2) {
			if (validate3 != validate4) {
				return true;
			}
		}
		return false;
	}

	// Validate the intersection for given two points
	public static int validateIntersection(SpatialPoint p, SpatialPoint q,
			SpatialPoint r) {
		float val = (q.getPointY() - p.getPointY())
				* (r.getPointX() - q.getPointX())
				- (q.getPointX() - p.getPointX())
				* (r.getPointY() - q.getPointY());
		if (val == 0.0) {
			return 0;
		}
		return (val > 0) ? 1 : 2;
	}

	// Function to split the given data based on "," separator
	public static Float[] splitingStringToFloat(String inputData,
			String Separator) {
		String[] SplitString = inputData.split(Separator);
		Float[] fnew = new Float[SplitString.length];
		for (int i = 0; i < SplitString.length; i++) {
			fnew[i] = Float.parseFloat(SplitString[i]);
		}
		return fnew;
	}
}