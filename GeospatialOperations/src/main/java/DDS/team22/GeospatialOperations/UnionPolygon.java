package DDS.team22.GeospatialOperations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
	
public class UnionPolygon {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Union Polygon");
		JavaSparkContext sc = new JavaSparkContext(conf);
		unionPolygons(sc,"input_data/UnionQueryTestData.csv", "output_data/UnionQueryTestResult_"+Utils.getCurrentTime());
		sc.close();
	}
	
	public static void unionPolygons(JavaSparkContext sc, String input_file,String output_file) throws IOException{ 
		JavaRDD<String> input_data = sc.textFile(input_file);
		JavaRDD<Geometry> poly_rdd = input_data.mapPartitions(LocalUnion);
		Collection<Geometry> poly_list = poly_rdd.collect();
		CascadedPolygonUnion cascaded_polygons = new CascadedPolygonUnion(poly_list);
		Coordinate[] coordinates = cascaded_polygons.union().getCoordinates();
		List<String> result = new ArrayList<String>();
		for (int i=0;i<coordinates.length-1;i++){
			result.add(coordinates[i].x+", "+coordinates[i].y);
		}
		JavaRDD<String> global_output = sc.parallelize(result).repartition(1);
		global_output.saveAsTextFile(output_file);
	}

	public static FlatMapFunction<Iterator<String>, Geometry> LocalUnion = new FlatMapFunction<Iterator<String>, Geometry>(){
		private static final long serialVersionUID = 1L;

		public Iterable<Geometry> call(Iterator<String> input_data) {
			List<Geometry> polygons =  new ArrayList<Geometry>();
			while (input_data.hasNext()){
				String[] coordinates = input_data.next().split(",");
				double x1 = Double.parseDouble(coordinates[0]);
				double y1 = Double.parseDouble(coordinates[1]);
				double x2 = Double.parseDouble(coordinates[2]);
				double y2 = Double.parseDouble(coordinates[3]);
				GeometryFactory geom = new GeometryFactory();
				Geometry polygon = geom.createPolygon(new Coordinate[]{
						new Coordinate(x1,y1),
						new Coordinate(x1,y2),
						new Coordinate(x2,y2),
						new Coordinate(x2,y1),
						new Coordinate(x1,y1)
				});
				polygons.add(polygon);
			}
			CascadedPolygonUnion cascaded_polygons = new CascadedPolygonUnion(polygons);
			Iterable<Geometry> poly_union = Arrays.asList(cascaded_polygons.union());
			return poly_union;
		}
	};
}