import java.io.File;

public class InfoFile {

	public static void main(String...args)
{

		File f = new File("D:file.txt");
		 
		if(f.exists()) {
		// get file name
		System.out.println("name of the file is: "+f.getName());
		
		//get path of the file
		System.out.println("path of the file is : "+f.getAbsolutePath());
		
		//check if file is writable or not
		System.out.println("is this file is writable or not? "+f.canWrite());
		
		//check of the file is readable or not
		System.out.println("is this file readable or not? "+f.canRead());
		
		// find the size of the file
		System.out.println("the size of the file is "+f.length());
		}else {
			
			System.out.println("file "+f.getName()+" does not exist");
		}

}
}
