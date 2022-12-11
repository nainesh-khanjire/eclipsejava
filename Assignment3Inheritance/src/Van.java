import java.util.Scanner;

public class Van extends Vehicle {

	int doors;
	double load;
	int numberofboxes; // this is SUBCLASS

	public Van(int wheels, String color, String fuel, double speed, int doors) {
		super(wheels, color, fuel, speed);
		this.doors = doors;

	}

	public void loadVan() {
		System.out.println("This is van class >> ");
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter number of boxes loaded van: ");
		numberofboxes = sc.nextInt();
		load = numberofboxes * 50; // 50 kg per box
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

	public int getNumberofboxes() {
		return numberofboxes;
	}

	public void setNumberofboxes(int numberofboxes) {
		this.numberofboxes = numberofboxes;
	}

	void start() {
		System.out.println("Start at 20km/hr");

	}

	void print() {
		System.out.println("van has " + doors + " doors");
		System.out.println("the total load in the van is: " + load + " kg");
	}
}
