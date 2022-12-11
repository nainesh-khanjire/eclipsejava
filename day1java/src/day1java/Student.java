package day1java;

public class Student {


	public static String FindResult(int mark) {
		String result;
		
		if(mark>50)
		{
			result ="pass";
		}else 
			result = "fail";
		System.out.println(result);
		
		return result;
		
		}
	
	
//	public static int FindEligible(int Degreepercent) {
//		
//		int status;
//		if(Degreepercent>55)
//		{
//			status = 1;
//		} 
//		else
//		{
//			status = 0;
//		}
//		return  status;
//		
//		}
//	
	
public static boolean FindEligible(int Degreepercent) {
		
		boolean status;
		if(Degreepercent>55)
		{
			status = true;
		} 
		else
		{
			status = false;
		}
		return status;
		
		}
	
	
	
	public static void main(String[] args) {
	
		int stu= 1000;
		String name = "nainesh";
		int mark= 70;
		int DegreePercent = 60;
		System.out.println(stu);
		System.out.println(name);
		System.out.println(mark);

		String result = FindResult(mark);
		System.out.println(result);
	
		boolean status = FindEligible(DegreePercent);
	System.out.println(status);
	
	if(status) 
	{
		System.out.println("eligible");
	}else
	{
		System.out.println("not eligible");
	}
	}
		
	}
