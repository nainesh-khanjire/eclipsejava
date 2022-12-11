import java.io.File;

public class DeleteFile {
public static void main(String...args) {
	
	File f = new File("D:file.txt");
	if(f.delete()) {
		
		System.out.println("the file "+f.getName()+" has been deleted successfully !");
		
	}else {
		
		System.out.println("unable to delete file :-/");
	}
	
}
}
