package DDS.team22.GeospatialOperations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
public class Helper {
	
	public static String ConvexHull(JavaSparkContext sc) throws IOException{
		JavaRDD<String>points=sc.textFile("ConvexHullTestData.csv");		
		JavaRDD<Coordinate>local=Helper.calculateConvexHull(points);
		JavaRDD<Coordinate> localList = local.repartition(1);
		
		JavaRDD<Coordinate>globalList = localList.mapPartitions(new GlobalHull());
		String result="";
		List<Coordinate>list=globalList.collect();
		list.remove(list.size()-1);
		for(Coordinate cor : list){
			result+=cor.x+","+cor.y+"\n";
		}
		
		File file=new File("convexHullResult.txt");
		FileWriter fw;
		if(file.exists())
			fw=new FileWriter(file);
		else{
			file.createNewFile();
			fw=new FileWriter(file);
		}
		fw.write(result);
		fw.close();
		return result;
	}

	public static JavaRDD<Coordinate>calculateConvexHull(JavaRDD<String>points){
		JavaRDD<Coordinate>local=points.mapPartitions(new LocalConvexHull());
		return local;
	}
}

class LocalConvexHull implements FlatMapFunction<Iterator<String>,Coordinate >,Serializable{

 	private static final long serialVersionUID = 1L;
	public Iterable<Coordinate> call(Iterator<String> s) throws Exception {
		List<Coordinate>coord_list=null;
		try{
			coord_list=new ArrayList<Coordinate>();
			while(s.hasNext()){
				String line=s.next();
				String[]coord_arr=line.split(",");
				Double x=Double.parseDouble(coord_arr[0]);
				Double y=Double.parseDouble(coord_arr[1]);
				Coordinate coord=new Coordinate(x, y);
				coord_list.add(coord);
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		
		GeometryFactory geom=new GeometryFactory();
		Coordinate[]coord_arr=coord_list.toArray(new Coordinate[coord_list.size()]);
		ConvexHull ch=new ConvexHull(coord_arr,geom);
		Geometry geometry=ch.getConvexHull();
		Coordinate[]result=geometry.getCoordinates();
		List<Coordinate>li=Arrays.asList(result);
		
		
		return li;
	}
}

class GlobalHull implements FlatMapFunction<Iterator<Coordinate>, Coordinate>, Serializable
{
	private static final long serialVersionUID = 1L;

	//Iterates over all the partitions and iteratively calcualtes the convex hull
	public Iterable<Coordinate> call(Iterator<Coordinate> coordinates)
	{
		List<Coordinate> list = new ArrayList<Coordinate>();
		GeometryFactory geom = new GeometryFactory();
		while(coordinates.hasNext())
		{
			list.add(coordinates.next());
		}
		ConvexHull ch = new ConvexHull(list.toArray(new Coordinate[list.size()]), geom);
		Geometry g=ch.getConvexHull();
		Coordinate[] c= g.getCoordinates();
		
		//Convert the coordinates array to arraylist here
		List<Coordinate> arrList = Arrays.asList(c);
		return arrList;
	}
}