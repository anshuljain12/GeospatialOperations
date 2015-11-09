package DDS.team22.GeospatialOperations;

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

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
	
public class UnionPolygon {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("Union Polygon");
		JavaSparkContext sc = new JavaSparkContext(conf);
		if (args.length<2){
			System.out.println("Insufficient number of inputs");
			sc.close();
			return;
		}
		unionPolygons(sc,args[0], args[1]+Utils.getCurrentTime());
		sc.close();
	}
	
	public static void unionPolygons(JavaSparkContext sc, String input_file,String output_file) throws IOException{ 
		JavaRDD<String> input_data = sc.textFile(input_file);
		JavaRDD<Geometry> poly_rdd = input_data.mapPartitions(LocalUnion);
		JavaRDD<Geometry> poly_rdd_rep = poly_rdd.repartition(1);
		JavaRDD<String> final_polygons = poly_rdd_rep.mapPartitions(GlobalUnion).repartition(1).distinct();

		final_polygons.saveAsTextFile(output_file);
	}

	public static FlatMapFunction<Iterator<Geometry>, String> GlobalUnion = new FlatMapFunction<Iterator<Geometry>, String>(){
		private static final long serialVersionUID = 1L;

		public Iterable<String> call(Iterator<Geometry> local_polygons) {
			List<Geometry> polygons =  new ArrayList<Geometry>();
			while (local_polygons.hasNext()){
				polygons.add(local_polygons.next());
			}
			CascadedPolygonUnion cascaded_polygons = new CascadedPolygonUnion(polygons);
			Coordinate[] coordinates = cascaded_polygons.union().getCoordinates();
			List<String> coordinates_list = new ArrayList<String>();
			for (Coordinate c : coordinates){
				coordinates_list.add(c.x+", "+c.y);
			}
			return coordinates_list;
		}
	};
	
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