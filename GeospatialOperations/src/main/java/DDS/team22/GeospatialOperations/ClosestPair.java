package DDS.team22.GeospatialOperations;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class ClosestPair {
	public static void main(String[] args) throws IOException {

		System.out.println("Starting with closest pair of points");
		if (args.length<2){
			System.out.println("Insufficient number of inputs");
			return;
		}
		String logFile = args[0];
		SparkConf conf = new SparkConf().setAppName("Closest Pair");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile);
        
        System.out.println("Local closest pair");
        JavaRDD<PointDouble> lines = logData.mapPartitions(new FlatMapFunction<Iterator<String>,PointDouble>(){
        	
        	private static final long serialVersionUID = 1L;
			public Iterable<PointDouble> call(Iterator<String> arg0) throws Exception {
				
				ClosestPair closestPairObj = new ClosestPair();
				
				List<PointDouble> points = new ArrayList<PointDouble>();
				List<PointDouble> finalClosestPair = new ArrayList<PointDouble>();
				List<Coordinate> coordinates = new ArrayList<Coordinate>();
				GeometryFactory geometryFactory = new GeometryFactory();
				
				while(arg0.hasNext()){
					String str = arg0.next();
					String[] array = str.split(",");
					
					PointDouble point = new PointDouble();
					point.setxCoordinate(Double.parseDouble(array[0]));
					point.setyCoordinate(Double.parseDouble(array[1]));
					points.add(point);
					
					Coordinate coordinate = new Coordinate(Double.parseDouble(array[0]),Double.parseDouble(array[1]));
					coordinates.add(coordinate);
				}
					// Find Convex Hull
					ConvexHull convexHull = new ConvexHull(coordinates.toArray(new Coordinate[coordinates.size()]), geometryFactory);
					Geometry geometry = convexHull.getConvexHull();
					List<Coordinate> localConvexHull =  Arrays.asList(geometry.getCoordinates());
					Set<Coordinate> sett=new HashSet<Coordinate>(localConvexHull);
					ClosestPairOfPoints closestPair = closestPairObj.closest(points);
				
					if(!sett.contains(new Coordinate(closestPair.closestPoint1.getxCoordinate() , closestPair.closestPoint1.getyCoordinate())))
						finalClosestPair.add(closestPair.closestPoint1);
					if(!sett.contains(new Coordinate(closestPair.closestPoint2.getxCoordinate() , closestPair.closestPoint2.getyCoordinate())))
						finalClosestPair.add(closestPair.closestPoint2);
						
					for(Coordinate coordinate2 : sett){
						
						PointDouble pointNew = new PointDouble();
						pointNew.setxCoordinate(coordinate2.x);
						pointNew.setyCoordinate(coordinate2.y);
						
						finalClosestPair.add(pointNew);
					}
				
			return finalClosestPair;
        }
	});
        
        System.out.println("Global closest pair");
        JavaRDD<PointDouble> localPointsList = lines.repartition(1);
        JavaRDD<PointDouble> FinalClosetPairList = localPointsList.mapPartitions(new FlatMapFunction<Iterator<PointDouble>, PointDouble>(){
        	
        	private static final long serialVersionUID = 1L;
			public Iterable<PointDouble> call(Iterator<PointDouble> arg0) throws Exception {
				
				ClosestPair closestPairObj = new ClosestPair();
				
				List<PointDouble> listPoints = new ArrayList<PointDouble>();
				while (arg0.hasNext()){
					listPoints.add(arg0.next());
				}
				
				ClosestPairOfPoints finalClosetstPairOfPoints = closestPairObj.closest(listPoints);
				
				List<PointDouble> geoSpatialClosestPoints = new ArrayList<PointDouble>();
				geoSpatialClosestPoints.add(finalClosetstPairOfPoints.closestPoint1);
				geoSpatialClosestPoints.add(finalClosetstPairOfPoints.closestPoint2);

		        return geoSpatialClosestPoints;
		        }
		});
        
        JavaRDD<PointDouble> points = FinalClosetPairList.repartition(1);
        String output_folder = args[1]+Utils.getCurrentTime();
        List<PointDouble> point_list = points.collect();
        List<String> result = new ArrayList<String>();
        for (PointDouble p : point_list){
        	result.add(p.getxCoordinate()+", "+p.getyCoordinate());
        }
        JavaRDD<String> global_result = sc.parallelize(result).repartition(1);
        global_result.saveAsTextFile(output_folder);
        sc.close();
    }

	public ClosestPairOfPoints bruteForce(List<PointDouble> listOfPoints){
		
		ClosestPairOfPoints closestPairDetails = new ClosestPairOfPoints();
		
		int listSize = listOfPoints.size();
		for(int i = 0; i<listSize ; i++){
			for(int j=i+1 ; j<listSize; j++){
				if(euclideanDistance(listOfPoints.get(i), listOfPoints.get(j)) != 0){
				if(euclideanDistance(listOfPoints.get(i), listOfPoints.get(j))<closestPairDetails.shortestDistance){
					closestPairDetails.closestPoint1 = listOfPoints.get(i);
					closestPairDetails.closestPoint2 = listOfPoints.get(j);
					closestPairDetails.shortestDistance = euclideanDistance(listOfPoints.get(i), listOfPoints.get(j));
				}}
			}
		}
		return closestPairDetails;
	}
    
	public double euclideanDistance(PointDouble point1,PointDouble point2){
		double xDistance = point1.getxCoordinate() - point2.getxCoordinate();
		double yDistance = point1.getyCoordinate() - point2.getyCoordinate();
		return(Math.sqrt((xDistance * xDistance) + (yDistance * yDistance)));
	}
	
	public ClosestPairOfPoints closest(List<PointDouble> listOfPoints){
		ClosestPairOfPoints closestPairDetails = new ClosestPairOfPoints();
		int listSize = listOfPoints.size();
		for(int i = 0; i<listSize ; i++){
			for(int j=i+1 ; j<listSize; j++){
				if(euclideanDistance(listOfPoints.get(i), listOfPoints.get(j))<closestPairDetails.shortestDistance){
					closestPairDetails.closestPoint1 = listOfPoints.get(i);
					closestPairDetails.closestPoint2 = listOfPoints.get(j);
					closestPairDetails.shortestDistance = euclideanDistance(listOfPoints.get(i), listOfPoints.get(j));
				}
			}
		}
		return closestPairDetails;
	}
	
}

class PointDouble implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private double xCoordinate;
	private double yCoordinate;
	
	public double getxCoordinate() {
		return xCoordinate;
	}
	public void setxCoordinate(double xCoordinate) {
		this.xCoordinate = xCoordinate;
	}
	public double getyCoordinate() {
		return yCoordinate;
	}
	public void setyCoordinate(double yCoordinate) {
		this.yCoordinate = yCoordinate;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PointDouble other = (PointDouble) obj;
		if (Double.doubleToLongBits(xCoordinate) != Double
				.doubleToLongBits(other.xCoordinate))
			return false;
		if (Double.doubleToLongBits(yCoordinate) != Double
				.doubleToLongBits(other.yCoordinate))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Point [xCoordinate=" + xCoordinate + ", yCoordinate="
				+ yCoordinate + "]";
	}
}

class ClosestPairOfPoints implements Serializable{
	private static final long serialVersionUID = 1L;
	
	public ClosestPairOfPoints() {
		closestPoint1=new PointDouble();
		closestPoint2=new PointDouble();
	}
	public PointDouble closestPoint1;
	public PointDouble closestPoint2;
	public double shortestDistance = Double.MAX_VALUE;	
}