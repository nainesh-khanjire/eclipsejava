import java.util.Scanner;

public class Teacher extends Person {

	
	void read() {
		System.out.println("Enter your name and age >>");
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter name: ");
		name = sc.next();
		System.out.println("Enter age: ");
		age = sc.nextInt();

	}

void disp() {
		
		System.out.println("My name is "+name);
	}
	
	void myprofession() {
		System.out.println("MY PROFESSION IS TEACHING");

	}

}
