package com.boom.marketUpdate.adr.bean;

public class TupleBean<X, Y> { 
	
	/* Data Structure - Tuple */
	
    public final X x; 
    public final Y y; 
    
    public TupleBean(X x, Y y) { 
        this.x = x; 
        this.y = y; 
    }

    @Override
    public String toString() {
        return "(" + x + "|" + y + ")";
    }
    
    public X getX(){
    	return this.x;
    }
    
    public Y getY(){
    	return this.y;
    }
}