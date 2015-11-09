package DDS.team22.GeospatialOperations;

import java.io.Serializable;
import java.sql.Timestamp;

public class Utils {

	public static Float[] splitingStringToFloat(String inputData,String Separator){
        String[] SplitString=inputData.split(Separator); 
        Float[] fnew= new Float[SplitString.length];
        for(int i=0;i<SplitString.length;i++){
            fnew[i]=Float.parseFloat(SplitString[i]);
        }
           return fnew;
    }
	
	public static Long getCurrentTime() {
		return System.currentTimeMillis();
	}
	
}

class PointDouble implements Serializable,Comparable<PointDouble>{
	
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
	
	public int compareTo(PointDouble p) {
		if (this.xCoordinate == p.xCoordinate) {
				if(this.yCoordinate>p.yCoordinate)
					return 1;
				else if(this.yCoordinate<p.yCoordinate) 
					return -1;
				else 
					return 0;
		} else if(this.xCoordinate>p.xCoordinate) {
			return 1;
		}
		else
			return -1;
	} 
}