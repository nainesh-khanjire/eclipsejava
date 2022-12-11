
public class Vehicle {

	int wheels;
	String color;
	String fuel;
	double speed;

	public Vehicle(int wheels, String color, String fuel, double speed) {
		super();
		this.wheels = wheels;
		this.color = color;
		this.fuel = fuel;
		this.speed = speed;
	}

	// this is SUPER CLASS/PARENT CLASS
	void start() {
		System.out.println("this vehicle started moving");
	}

	void accelerate() {
		System.out.println("this vehicle is in high speed");
	}

	void brake() {
		System.out.println("this vehicle is stopped");
	}

	public int getWheels() {
		return wheels;
	}

	public void setWheels(int wheels) {
		this.wheels = wheels;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}

	public String getFuel() {
		return fuel;
	}

	public void setFuel(String fuel) {
		this.fuel = fuel;
	}

	public double getSpeed() {
		return speed;
	}

	public void setSpeed(double speed) {
		this.speed = speed;
	}

}
