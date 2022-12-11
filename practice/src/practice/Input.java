import java.util.ArrayList;
import java.util.Arrays;

public class Input {
	public static void main(String... args) {
	

		String[] f = {"nainesh", "nitin","x", "y","z"};
		
	
		ArrayList<String> arlist = new ArrayList<>(Arrays.asList("nainesh", "nitin","x", "y","z"));

		// get element
		System.out.println(f[1]);
		System.out.println(arlist.get(1));
		
		// get size
		System.out.println(f.length);
		System.out.println(arlist.size());
		// size is a method call
		//add element
		arlist.add("snehal");
		 System.out.println(arlist.get(5));

		 //set an element
		 f[2] = "aditya";
		 System.out.println(f[2]);
		 
		 
		 arlist.add("aditya");
		 arlist.add("sanket");
		 arlist.remove("x");
		 arlist.remove("z");
		 arlist.remove("y");
		
	System.out.println(" >> removed elements x y and z");
		for(int i =1;i<arlist.size();i++) {
		 
		 System.out.println(arlist.get(i));
		}

		// print
		System.out.println(f);
		// this prints the memory address of the elements
		System.out.println(arlist);
		
	}

}
