import java.io.File;
import java.io.IOException;

public class CreateFile {

	public static void main(String...args) {
		File f  = new File("D:file.txt");
		try {
			f.createNewFile();
			System.out.println(f.getName()+" file is created successfully");
		} catch (IOException e) {
		System.out.println("unable to create new file");
			e.printStackTrace();
		}
		
		
		
		
	}
}
