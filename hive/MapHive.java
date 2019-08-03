package com.zbdx.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.zbdx.dao.HiveDao;

public class MapHive extends HiveDao {
	public Configuration conf() {
		Configuration cfg = HBaseConfiguration.create();
		cfg.set("hbase.zookeeper.quorum", "192.168.210.128");
		return cfg;
	}

	public void drop() {
		Connection conn = super.getConnection();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			String sql = "drop table user_hqh";
			rs = st.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			super.closeConnection(conn, st, rs);
		}
	}

	public void createTable() {
		Connection conn = super.getConnection();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			String sql = "create table " + "user_hqh" + "(" + "userid string,"
					+ "ujing double," + "uwei double)"
					+ "row format delimited " + "fields terminated by '\t' "
					+ "stored as textfile";
			rs = st.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			super.closeConnection(conn, st, rs);
		}

	}

//	public void pathNum() {
//		int reduceNum = 0;
//		while (reduceNum < 6) {
//			load("/student_test/hqh/music/music_out/part-r-0000" + reduceNum);
//			reduceNum++;
//		}
//	}

	public void load(String filePath) {
		Connection conn = super.getConnection();
		Statement st = null;
		ResultSet rs = null;
		
//		filePath="/opt/data/data.txt";

		String sql = "load data  inpath '" + filePath + "' into table user_hqh";
		System.out.println("Running:" + sql + ":");
		try {
			st = conn.createStatement();
			rs = st.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			super.closeConnection(conn, st, rs);
		}
	}
}
