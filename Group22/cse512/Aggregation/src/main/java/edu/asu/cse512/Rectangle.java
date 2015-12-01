package edu.asu.cse512;

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

	public Rectangle(float xLeftLower, float yLeftLower, float xRightUpper,
			float yRightUpper) {

		leftLower = new SpatialPoint(xLeftLower, yLeftLower);
		rightUpper = new SpatialPoint(xRightUpper, yRightUpper);

		leftUpper = new SpatialPoint(xLeftLower, yRightUpper);
		rightLower = new SpatialPoint(xRightUpper, yLeftLower);

	}

	public Rectangle(float id, float xLeftLower, float yLeftLower,
			float xRightUpper, float yRightUpper) {

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

	//Helper method to check if the rectangle is inside
	public boolean findIfPointIsInside(SpatialPoint point) {
		float pointX = point.pointX;
		float pointY = point.pointY;

		if (pointX > this.leftLower.pointX && pointY > this.leftLower.pointY
				&& pointX > this.leftUpper.pointX
				&& pointY < this.leftUpper.pointY
				&& pointX < this.rightLower.pointX
				&& pointY > this.rightLower.pointY
				&& pointX < this.rightUpper.pointX
				&& pointY < this.rightUpper.pointY) {

			return true;

		} else {
			return false;
		}

	}

	// Helper method to check if two rectangles are equal
	public boolean RectangleEquals(Rectangle rectangle) {
		if (rectangle.leftLower.pointX == this.leftLower.pointX
				&& rectangle.leftLower.pointY == this.leftLower.pointY
				&& rectangle.rightUpper.pointX == this.rightUpper.pointX
				&& rectangle.rightUpper.pointY == this.rightUpper.pointY) {
			return true;
		} else {
			return false;
		}

	}

	//helper method to find if a point is inside or on the rectangle
	public boolean findIfPointIsInsideforPoint(SpatialPoint point) {
		float pointX = point.pointX;
		float pointY = point.pointY;

		if (pointX >= this.leftLower.pointX && pointY >= this.leftLower.pointY
				&& pointX >= this.leftUpper.pointX
				&& pointY <= this.leftUpper.pointY
				&& pointX <= this.rightLower.pointX
				&& pointY >= this.rightLower.pointY
				&& pointX <= this.rightUpper.pointX
				&& pointY <= this.rightUpper.pointY) {

			return true;

		} else {
			return false;
		}

	}

	public String toString() {
		return leftLower + ", " + rightLower + ", " + rightUpper + ", "
				+ leftUpper;
	}
}
