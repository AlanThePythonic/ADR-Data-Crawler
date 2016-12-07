package com.boom.marketUpdate.adr.bean;

public class TupleBeanTriple<X, Y, Z> extends TupleBean<X, Y> {

	/*
	 * Data Structure which extended from tupleBean to make one more data
	 * storage
	 */

	final public Z z;

	public TupleBeanTriple(X x, Y y, Z z) {
		super(x, y);
		this.z = z;
	}

	@Override
	public String toString() {
		return "(" + x + "|" + y + "|" + z + ")";
	}

	public Z getZ() {
		return this.z;
	}

}
