package jdbc;

import java.sql.*;

public class Insertjdbc {
	public static void main(String... args) {
		// create connection
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");

			String url = "jdbc:mysql://localhost:3306/javatest1";
			String username = "root";
			String password = "root";

			Connection con = DriverManager.getConnection(url, username, password);

			if (con.isClosed()) {

				System.out.println("not connected");
			} else {

				System.out.println("connection successfull !");
			}
			// create query
			
			
		} catch (Exception e) {
			System.out.println("");
		}

	}
}
