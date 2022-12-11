import java.util.Scanner;

public class Teacher extends Person {
	public Teacher(int age, String name) {
		super(age, name);

	}

	void myprofession() {
		System.out.println("TEACHER");
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter your name: ");
		name = sc.next();
		System.out.println("my name is " + name);
		System.out.println("I am a Teacher");

	}
}