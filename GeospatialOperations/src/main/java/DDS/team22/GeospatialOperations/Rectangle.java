package DDS.team22.GeospatialOperations;

public class Rectangle implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	SpatialPoint leftLower;
	SpatialPoint leftUpper;
	SpatialPoint rightLower;
	SpatialPoint rightUpper;
	float id;

	public SpatialPoint getLeftLower() {
		return this.leftLower;
	}

	public void setLeftLower(SpatialPoint leftLower) {
		this.leftLower = leftLower;
	}

	public SpatialPoint getLeftUpper() {
		return this.leftUpper;
	}

	public void setLeftUpper(SpatialPoint leftUpper) {
		this.leftUpper = leftUpper;
	}

	public SpatialPoint getRightLower() {
		return this.rightLower;
	}

	public void setRightLower(SpatialPoint rightLower) {
		this.rightLower = rightLower;
	}

	public SpatialPoint getRightUpper() {
		return this.rightUpper;
	}

	public void setRightUpper(SpatialPoint rightUpper) {
		this.rightUpper = rightUpper;
	}

	public float getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Rectangle(float xLeftLower, float yLeftLower, float xRightUpper, float yRightUpper) {

		leftLower = new SpatialPoint(xLeftLower, yLeftLower);
		rightUpper = new SpatialPoint(xRightUpper, yRightUpper);

		leftUpper = new SpatialPoint(xLeftLower, yRightUpper);
		rightLower = new SpatialPoint(xRightUpper, yLeftLower);

	}

	public Rectangle(float id, float xLeftLower, float yLeftLower, float xRightUpper, float yRightUpper) {

		this.id = id;
		if (xLeftLower > xRightUpper) {
			float temp = xLeftLower;
			xLeftLower = xRightUpper;
			xRightUpper = temp;
		}
		if (yLeftLower > yRightUpper) {
			float temp = yLeftLower;
			yLeftLower = yRightUpper;
			yRightUpper = temp;
		}
		leftLower = new SpatialPoint(xLeftLower, yLeftLower);
		rightUpper = new SpatialPoint(xRightUpper, yRightUpper);
		leftUpper = new SpatialPoint(xLeftLower, yRightUpper);
		rightLower = new SpatialPoint(xRightUpper, yLeftLower);

	}

	public boolean findIfPointIsInside(SpatialPoint point) {
		float pointX = point.pointX;
		float pointY = point.pointY;

		if (pointX > this.leftLower.pointX && pointY > this.leftLower.pointY && pointX > this.leftUpper.pointX
				&& pointY < this.leftUpper.pointY && pointX < this.rightLower.pointX && pointY > this.rightLower.pointY
				&& pointX < this.rightUpper.pointX && pointY < this.rightUpper.pointY) {

			return true;

		} else {
			return false;
		}

	}

	public boolean findIfPointIsInsideforPoint(SpatialPoint point) {
		float pointX = point.pointX;
		float pointY = point.pointY;

		if (pointX >= this.leftLower.pointX && pointY >= this.leftLower.pointY && pointX >= this.leftUpper.pointX
				&& pointY <= this.leftUpper.pointY && pointX <= this.rightLower.pointX
				&& pointY >= this.rightLower.pointY && pointX <= this.rightUpper.pointX
				&& pointY <= this.rightUpper.pointY) {

			return true;

		} else {
			return false;
		}

	}

	public String toString() {
		return leftLower + ", " + rightLower + ", " + rightUpper + ", " + leftUpper;
	}
}
