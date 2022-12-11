
public class Employee {

	int SSN;
	String firstname;
	String lastname;

	Employee() { // user defined, parameterless constructor

		SSN = 0;
		firstname = "DEFAULT";
		lastname = "DEFAULT";
	}

	public void setSSN(int sSN) {
		SSN = sSN;
	}

	public int getSSN() {
		return SSN;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	void print() {
		System.out.println("Employee name is: " + firstname + " " + lastname);
		System.out.println("Employee SSN is: "+SSN);
	}

}
