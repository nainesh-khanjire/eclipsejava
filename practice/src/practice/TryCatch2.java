package practice;

public class TryCatch2 {

	public static void main(String[] args) {
		
		
		try 
		{
			int a = 10/0;
			
			
		}catch(Exception a) {		
				
			System.out.println(a);
			
		}
		
		System.out.println("NUMBERS CANNOT BE DIVIDED BY ZERO!");
		
	}
}
