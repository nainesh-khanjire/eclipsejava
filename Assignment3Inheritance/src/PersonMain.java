
public class PersonMain {

	public static void main(String[] args) {
		/*
		 * Write java program to implement Inheritance with following example: Person
		 * will have name and age as data members. Teacher and employee will inherit
		 * data members in the super class and create its own method myProfession() to
		 * display their profession. Then create objects of Teacher, Permanent, and
		 * Contract employee to display their profession..
		 */

		Person p = new Person(20, null);

		Emp e = new Emp(0, null);
		e.myprofession();

		Teacher t = new Teacher(0, null);
		t.myprofession();

		PermanentEmp pe = new PermanentEmp(0, null);
		pe.myprofession();

		ContractEmp ce = new ContractEmp(0, null);
		ce.myprofession();

	}

}
