
public class VehicleMain {

	public static void main(String[] args) {

		Van v = new Van(4, "Black", "Petrol", 45, 3);
		v.start();
		v.loadVan();

		Truck t = new Truck(10, "Blue", "diesel", 40);
		t.start();
		t.loadContainer();

		Car c = new Car(4, "Red", "Petrol", 50);
		c.start();

		Bike b = new Bike(2, "white-red", "petrol", 60);
		b.start();

	}
}
