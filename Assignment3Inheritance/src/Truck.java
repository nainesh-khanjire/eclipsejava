import java.util.Scanner;

public class Truck extends Vehicle {

	int doors;
	double load;
	int sizeofcontainer;

	public Truck(int wheels, String color, String fuel, double speed) {
		super(wheels, color, fuel, speed);
		this.doors = 2;

	}

	void loadContainer() {
		System.out.println("This is Truck class >> ");
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter number of container loaded van: ");
		sizeofcontainer = sc.nextInt();
		load = sizeofcontainer * 500; // 500 kg per container
		print();

	}

	public int getDoors() {
		return doors;
	}

	public void setDoors(int doors) {
		this.doors = doors;
	}

	public double getLoad() {
		return load;
	}

	public void setLoad(double load) {
		this.load = load;
	}

	public int getSizeofcontainer() {
		return sizeofcontainer;
	}

	public void setSizeofcontainers(int sizeofcontainer) {
		this.sizeofcontainer = sizeofcontainer;
	}

	void print() {

		System.out.println("Total load in the Truck is: " + load+" kgs");
	}

}
