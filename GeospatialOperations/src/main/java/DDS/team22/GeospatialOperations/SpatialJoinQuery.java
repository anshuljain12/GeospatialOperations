package DDS.team22.GeospatialOperations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SpatialJoinQuery {
	public static void main(String[] args) throws IOException {
		String inp1 = "input_data/JoinQueryInput2.csv";
		String inp2 = "input_data/JoinQueryInput1.csv";
		String out = "output_data/JoinQueryResult";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		spatialJoinQuery(inp1, inp2, out, sc);
		sc.close();
	}

	public static boolean spatialJoinQuery(String spatialObjectPathFile,
			String qryWindowsPath, String outputPath, JavaSparkContext sc)
			throws IOException {
		JavaRDD<String> inputData = sc.textFile(spatialObjectPathFile);
		JavaRDD<String> queryWindow = sc.textFile(qryWindowsPath);

		JavaRDD<Rectangle> rect = inputData
				.map(new Function<String, Rectangle>() {
					public Rectangle call(String s) {
						Float[] fnum = splitingStringToFloat(s, ",");
						return new Rectangle(fnum[0], fnum[1], fnum[2],
								fnum[3], fnum[4]);
					}
				});

		JavaRDD<Rectangle> rect1 = queryWindow
				.map(new Function<String, Rectangle>() {
					public Rectangle call(String s) {
						Float[] fnum = splitingStringToFloat(s, ",");
						return new Rectangle(fnum[0], fnum[1], fnum[2],
								fnum[3], fnum[4]);
					}
				});

		final Rectangle qryrect = rect1.first();
		Broadcast<List<Rectangle>> br = sc.broadcast(rect1.collect());
		final List<Rectangle> broad = br.value();

		JavaRDD<Tuple2<Long, List<Long>>> resultantRect = rect
				.map(new Function<Rectangle, Tuple2<Long, List<Long>>>() {
					public Tuple2<Long, List<Long>> call(Rectangle s) {

						List<Long> rectangleList = new ArrayList<Long>();
						for (Rectangle rec : broad) {

							if (rec.findIfPointIsInside(s.leftLower)
									&& rec.findIfPointIsInside(s.leftUpper)
									&& rec.findIfPointIsInside(s.rightLower)
									&& rec.findIfPointIsInside(s.rightUpper)) {
								float f = rec.getId();
								long l = Math.round(f);
								rectangleList.add(l);
							}
						}
						float f = s.getId();
						long l = Math.round(f);
						return new Tuple2<Long, List<Long>>(l, rectangleList);

					}
				});

		resultantRect.repartition(1).saveAsTextFile(outputPath);
		return false;

	}

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
