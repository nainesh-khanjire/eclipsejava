
public class Bike extends Vehicle {

	int seats;
	double saddleheight;

	public Bike(int wheels, String color, String fuel, double speed) {
		super(wheels, color, fuel, speed);
		this.seats = 2;
		this.saddleheight = 1; // in meters

		System.out.println("This is Bike class >> ");
	}

	public int getSeats() {
		return seats;
	}

	public void setSeats(int seats) {
		this.seats = seats;
	}

	public double getHeight() {
		return saddleheight;
	}

	public void setHeight(double saddleheight) {
		this.saddleheight = saddleheight;
	}

	void print() {

	}

}
