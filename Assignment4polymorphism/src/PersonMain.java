
public class PersonMain {

	public static void main(String[] args) {

		Employee e = new Employee();
		e.read();
		e.disp();
		e.myprofession();

		Teacher t = new Teacher();
		t.read();
		t.disp();
		t.myprofession();
		

		PermanentEmp p = new PermanentEmp();
		p.read();
		p.disp();
		p.myprofession();
		

		ContractEmp c = new ContractEmp();
		c.read();
		c.disp();
		c.myprofession();

	}

}
