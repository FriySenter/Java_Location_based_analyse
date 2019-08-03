package com.zbdx.test;

import com.zbdx.hive.MapHive;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MapHive map = new MapHive();
//		map.drop();
//		map.createTable();
		map.load("/opt/data/data.txt");
//		map.drop();

	}

}
