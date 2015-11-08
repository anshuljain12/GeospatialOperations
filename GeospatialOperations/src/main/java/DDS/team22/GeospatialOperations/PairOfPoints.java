package DDS.team22.GeospatialOperations;


import java.io.Serializable;

public class PairOfPoints implements Serializable {

	/**
	 * PairOfPoints and retrieve individual points from the pair
	 */
	//A unique ID that helps in serialization and de-serialization of this class
	private static final long serialVersionUID = 839823948924L;
	private Point p1;
	private Point p2;
	
	public PairOfPoints(Point p, Point q)
	{
		this.p1 = p;
		this.p2 = q;
	}
	
	public Point[] getPoint()
	{
		Point[] p = {this.p1,this.p2};
		return p;
	}
	
	
}
