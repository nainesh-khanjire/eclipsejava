import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class ReadFile {
public static void main(String...args) {
	
	File readFile = new File("D:file.txt");
	try {
	
		Scanner sc = new Scanner(readFile);
		//
		System.out.println("file read successfully");
		// using while loop
		while(sc.hasNextLine()) {
		
		String fileData = sc.nextLine();
		System.out.println(fileData);
	}
		sc.close();
		
	} catch (FileNotFoundException e) {
		
	System.out.println("unable to read file");
		
	e.printStackTrace();
	}
	
	
	
}
	
}
