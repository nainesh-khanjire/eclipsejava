package practice;

public class StaticBlock {


	static int a = 4;
	static int b;
		
	static  {
		System.out.println("Inside Static block");
		b= a *10;
	}

public static void main(String[] args) {
	StaticBlock s1 = new StaticBlock();
	System.out.println(StaticBlock.a+" "+StaticBlock.b);

    StaticBlock.b+=3;
    
	StaticBlock s2 = new StaticBlock();
	StaticBlock s3 = new StaticBlock();
	System.out.println(s1);
	System.out.println(s2);
	System.out.println(s3);
	System.out.println(StaticBlock.a+" "+StaticBlock.b); // 3 gets added to the b variable after modification of the value
// here the class StaticBlock ran only once when the class is loaded for the first time
	// when the first object is created
}

}
