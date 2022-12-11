package practice;

import java.util.*;
//import java.util.ArrayList;
//import java.util.Scanner;

public class Arlist {
	public static void main(String... args) {

		ArrayList<String> arlist = new ArrayList<String>(5);
		arlist.add("1. nainesh");
		arlist.add("2. nitin");
		arlist.add("3. sanket");
		arlist.add("4. snehal");

		System.out.println("print using print stsmt");
		System.out.println(arlist);

		System.out.println("print using for loop");
		for (int i = 1; i < arlist.size(); i++) {

			System.out.println(arlist.get(i));
		}

		System.out.println("Traversing through  iterator");

		Iterator itr = arlist.iterator();

		while (itr.hasNext()) {

			System.out.println(itr.next());
		}

		System.out.println("using for each loop");

		for (String names : arlist)
			System.out.println(names);

		
		
	}

}
