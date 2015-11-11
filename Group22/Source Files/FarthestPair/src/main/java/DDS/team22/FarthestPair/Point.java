package DDS.team22.FarthestPair;

import java.io.Serializable;

public class Point implements Serializable {
	/*
	 * Point Class from x and y coordinates and get distance between two points
	 */
	// A unique ID that helps in serialization and de-serialization of this
	// class
	private static final long serialVersionUID = 2834792472L;
	private float x;
	private float y;

	public Point(float x, float y) {
		this.x = x;
		this.y = y;
	}

	public float xcord() {
		return x;
	}

	public float ycord() {
		return y;
	}

	public static Double getDistPairOfPoints(Point P, Point Q) {
		double distance = Math.sqrt(Math.pow((P.x - Q.x), 2) + Math.pow(P.y - Q.y, 2));
		return distance;
	}
}