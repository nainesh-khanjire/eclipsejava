package practice;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class Fileio {
public static void main(String...args) {
	
try {
	BufferedWriter writer  = new BufferedWriter(new FileWriter("t1.txt"));
	Writer.write("this is line 1");
	Writer.close();
	

}catch(IOException e)
{
	e.printStackTrace();
		
}
}
	
}
