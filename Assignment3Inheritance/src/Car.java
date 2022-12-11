
public class Car extends Vehicle {

	int doors;
	int seats;

	public Car(int wheels, String color, String fuel, double speed) {
		super(wheels, color, fuel, speed);
		this.doors = 4;
		this.seats = 4;
		System.out.println("This is car class >> ");
	}

	public int getDoors() {
		return doors;
	}

	public void setDoors(int doors) {
		this.doors = doors;
	}

	public int getSeats() {
		return seats;
	}

	public void setSeats(int seats) {
		this.seats = seats;
	}

}
