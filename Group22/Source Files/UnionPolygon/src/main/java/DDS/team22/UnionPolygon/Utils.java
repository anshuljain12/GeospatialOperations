package DDS.team22.UnionPolygon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class Utils {

	public static Float[] splitingStringToFloat(String inputData, String Separator) {
		String[] SplitString = inputData.split(Separator);
		Float[] fnew = new Float[SplitString.length];
		for (int i = 0; i < SplitString.length; i++) {
			fnew[i] = Float.parseFloat(SplitString[i]);
		}
		return fnew;
	}

	public static Long getCurrentTime() {
		return System.currentTimeMillis();
	}

}

class PointDouble implements Serializable, Comparable<PointDouble> {

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
		if (Double.doubleToLongBits(xCoordinate) != Double.doubleToLongBits(other.xCoordinate))
			return false;
		if (Double.doubleToLongBits(yCoordinate) != Double.doubleToLongBits(other.yCoordinate))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Point [xCoordinate=" + xCoordinate + ", yCoordinate=" + yCoordinate + "]";
	}

	public int compareTo(PointDouble p) {
		if (this.xCoordinate == p.xCoordinate) {
			if (this.yCoordinate > p.yCoordinate)
				return 1;
			else if (this.yCoordinate < p.yCoordinate)
				return -1;
			else
				return 0;
		} else if (this.xCoordinate > p.xCoordinate) {
			return 1;
		} else
			return -1;
	}

	public static Function<String, PointDouble> ToPointDouble = new Function<String, PointDouble>() {
		private static final long serialVersionUID = 1L;

		public PointDouble call(String s) {
			String[] c = s.split(",");
			PointDouble p = new PointDouble();
			p.setxCoordinate(Double.parseDouble(c[0].trim()));
			p.setyCoordinate(Double.parseDouble(c[1].trim()));
			return p;
		}
	};

	public static FlatMapFunction<Iterator<PointDouble>, PointDouble> SortRDD = new FlatMapFunction<Iterator<PointDouble>, PointDouble>() {

		private static final long serialVersionUID = 1L;

		public Iterable<PointDouble> call(Iterator<PointDouble> point) throws Exception {
			List<PointDouble> point_list = new ArrayList<PointDouble>();
			while (point.hasNext()) {
				point_list.add(point.next());
			}
			Collections.sort(point_list);

			return point_list;
		}
	};

	public static Function<PointDouble, String> PointToString = new Function<PointDouble, String>() {

		private static final long serialVersionUID = 1L;

		public String call(PointDouble s) {

			StringBuffer res = new StringBuffer();
			res.append(s.getxCoordinate());
			res.append(",");
			res.append(s.getyCoordinate());
			return res.toString();
		}
	};
}