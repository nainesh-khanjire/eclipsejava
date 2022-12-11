import java.util.Scanner;

public class PermanentEmp extends Emp {
	public PermanentEmp(int age, String name) {
		super(age, name);
	}

	void myprofession() {

		System.out.println("PERMANENT EMPLOYEE");
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter your name: ");
		name = sc.next();
		System.out.println("my name is " + name);
		System.out.println("I am a permanent employee");

	}
}
