import java.io.FileWriter;
import java.io.IOException;

public class WriteFile {
	public static void main(String... args) {

		//adding block of try and  catch to catch and display error
		try {
			FileWriter fw = new FileWriter("D:file.txt");

			fw.write("this is first line\n"); // writing content to the file 
			fw.write("this is second line\n");
			fw.write("this is third line\n");
			
			fw.close(); // closing the stream
			System.out.println("content has been successfully written text to the file");
			// message to ensure that the try block has ran successfully to write into the file
		} catch (IOException e) {
			System.out.println("unable to write to the file");
				e.printStackTrace();
		}	

	}

}
