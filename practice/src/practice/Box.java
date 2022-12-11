package practice;

public class Box {
double l;
double h;
double w;


Box(){

	this.h = -1;
	this.l = -1;
	this.w = -1;
	 
	}
// cube
Box (double side, double w, double l){
	
	this.w = side;
	this.l= side;
	this.h = side;
}

Box (double side){
	
	this.w = side;
	this.l= side;
	this.h = side;
	}

Box (Box old){
	this.h = old.h;
	this.l = old.l;
	this.w = old.w;
	
}
public Box (Double l,Double h, Double w) {
	this.w = w;
	this.l = l;
	this.h = h;
	
}
public void info() {
	System.out.println("running the box");
}

}
