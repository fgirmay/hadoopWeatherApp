package com.cs523;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Year implements WritableComparable<Year> {
	
	private int year;
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		year = arg0.readInt();
	}


	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(year);
	}


	@Override
	public int compareTo(Year year) {
		// TODO Auto-generated method stub
		
		return Integer.compare(year.getYear(), this.year);
		
	}
	
	@Override
	public int hashCode() {
		
		final int prime = 31;
		int result = 1;
		result = prime * result + year;
		return result;
	}
	
	public int getYear() {
		return this.year;
	}
	
	public void setYear(int year) {
		this.year = year;
	}

}
