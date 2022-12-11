import java.util.Scanner;

public class ContractEmp extends Emp {

	public ContractEmp(int age, String name) {
		super(age, name);
	}

	void myprofession() {
		System.out.println("CONTRACT EMPLOYEE");
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter your name: ");
		name = sc.next();
		System.out.println("my name is " + name);
		System.out.println("I am a contract employee");

	}
}
