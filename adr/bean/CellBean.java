package com.boom.marketUpdate.adr.bean;

import java.util.ArrayList;

import jxl.Cell;

public class CellBean {

	/* A row of Data Source */
	private Cell[] row;

	/*
	 * Data Status : New Record = N, Deleted = D, Updated = U
	 */
	private String status;

	// Display which column has been changed
	private ArrayList<TupleBean<String, String>> differences;

	public CellBean(Cell[] row, String status, ArrayList<TupleBean<String, String>> differences) {
		this.row = row;
		this.status = status;
		this.differences = differences;
	}

	public Cell[] getRow() {
		return row;
	}

	public void setRow(Cell[] row) {
		this.row = row;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setDifference(ArrayList<TupleBean<String, String>> differences) {
		this.differences = differences;
	}

	public ArrayList<TupleBean<String, String>> getDifference() {
		return this.differences;
	}
}
