package com.bw.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import net.hydromatic.optiq.jdbc.Driver;

public class hivejdbc {
	public static void main(String[] args) throws Exception {
		
		Driver.class.forName("org.apache.hive.jdbc.HiveDriver");
		
		Connection conn = DriverManager.getConnection("jdbc:hive2://crxy77:10000/hsk", "root", "root");
		
		String sql = "select w.word as word,count(w.word) count from "
				+ "(select explode(split(line,' ')) as word from t_wc) w  "
				+ "group by w.word";
		
		PreparedStatement ps = conn.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		System.out.println("word\t\tcount");
		
		while (rs.next()) {
			String word = rs.getString("word");
			 int count = rs.getInt("count");
			 System.out.println(word+"\t\t"+count);
		}
		
		rs.close();
		ps.close();
		conn.close();
	}
}
