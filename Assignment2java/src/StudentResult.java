 import java.util.Scanner;

public class StudentResult {

	// assignment 2, student name, id, mark and result program
	String studentname;
	int studentid;
	static int mark;
	String result;

	public void ReadStudentData() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Please enter the following details >>");

		System.out.println("Enter student name ");
		studentname = sc.next();

		System.out.println("Enter student ID ");
		studentid = sc.nextInt();

		System.out.println("Enter the marks ");
		mark = sc.nextInt();
		sc.close();

	}

	public void DisplayStudentData() {

		System.out.println("The Student name is " + studentname);
		System.out.println("The Student ID is " + studentid);
		System.out.println("Marks of the student are " + mark);
	}

	public static String DisplayResult(int mark) {

		String result;

		if (mark >= 40) {
			result = "pass";

		} else
			result = "fail";

		return result;

	}

	public static void main(String[] args) {

		// assignment 2, student id, name,marks and result

		StudentResult s1 = new StudentResult();

		s1.ReadStudentData();
		s1.DisplayStudentData();
		s1.DisplayResult(mark);

		String result = DisplayResult(mark);
		System.out.println(result);

	}

}
