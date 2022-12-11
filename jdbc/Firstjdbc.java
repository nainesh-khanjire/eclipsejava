import java.sql.*;

public class Firstjdbc {
	public static void main(String[] args) {

		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/javatest1";
			String username = "root";
			String password = "root";

			Connection con = DriverManager.getConnection(url, username, password);
			
			
			if(con.isClosed()) {
				
				System.out.println("Connection is closed");

			}else {
				
				System.out.println("conneted to the sql server");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
