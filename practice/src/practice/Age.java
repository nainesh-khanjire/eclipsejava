package practice;

import java.util.Scanner;

public class Age {

	public static void main(String[] args) {
// storing 5 students in each classroom
		
		int age[][] = new int[2][5];
		Scanner sc = new Scanner(System.in);
		for (int i = 0; i <= age.length - 1; i++) {
			for (int j = 0; j <=age[i].length - 1; j++) {
				System.out.println("Enter the class " + i + " and student " + j);
				age[i][j] = sc.nextInt(); 
			
			}
		}
		for(int i=0;i<=age.length-1;i++) {
			
			for(int j=0;j<=age[i].length-1;j++) {
				
				
				System.out.println("class "+i+ " has "+" student "+j);
			}
		}
		
		
		
	}

}
