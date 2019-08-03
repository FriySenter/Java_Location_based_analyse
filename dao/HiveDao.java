package com.zbdx.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class HiveDao {
		Connection conn=null;
		Statement st =null;
		ResultSet rs=null;
		
		public Connection getConnection(){
			try {
				Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
				conn=DriverManager.getConnection("jdbc:hive://192.168.210.128:10000/default", "hadoop", "123456");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return conn;
		}
		
		public void closeConnection(Connection conn,Statement st,ResultSet rs){
			try {
				if(rs!=null){
					rs.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

}
