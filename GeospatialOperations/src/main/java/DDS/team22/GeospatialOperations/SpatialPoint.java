package DDS.team22.GeospatialOperations;

public class SpatialPoint implements java.io.Serializable {

	float pointX;
	float pointY;
	float id;
	
	public float getId() {
		return id;
	}

	public void setId(float id) {
		this.id = id;
	}

	public SpatialPoint(float id, float pointX, float pointY){
		this.id = id;
		this.pointX = pointX;
		this.pointY = pointY;
	}
	
	public String toString()
	{
		return ("("+this.pointX+", "+this.pointY+")");
	}
	
	public float getPointX() {
		return pointX;
	}

	public void setPointX(float pointX) {
		this.pointX = pointX;
	}

	public float getPointY() {
		return pointY;
	}

	public void setPointY(float pointY) {
		this.pointY = pointY;
	}

	public SpatialPoint(float pointX, float pointY){
		
		this.pointX = pointX;
		this.pointY = pointY;
		
	}
}
