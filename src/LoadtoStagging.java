import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.PreparedStatement;

public class LoadtoStagging {

	private  static void updateLogs() throws SQLException {

		String jdbcURL = "jdbc:mysql://localhost:3306";
		String username = "root";
		String password = "123456";
		//PreparedStatement statement = null;
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		Statement stmt = connect.connection.createStatement();
		String sql2 = "SET SQL_SAFE_UPDATES = 0";
		
		String sql4= "update sinhvien.hocsinh set sinhvien.hocsinh.Tên='ff' where sinhvien.hocsinh.STT like '1'";
		stmt.executeUpdate(sql2);
		stmt.executeUpdate(sql4);

	

	}

	public static void main(String[] args) throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306";
		String username = "root";
		String password = "123456";
		//PreparedStatement statement = null;
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		System.out.println("okkkkkkkkkk");

		Statement stmt = connect.connection.createStatement();
		System.out.println("SSSSS");
		String sql2 = "insert datamart.datamart select * from sinhvien.sinhvien ";
		
		String sql4="ALTER TABLE datamart.datamart ADD id varchar(50)"  ;
String kk="ddd";
		String sql5="UPDATE datamart.datamart SET  id = '"+kk+"'";
	
		System.out.println("ZZZZZZ");
		stmt.executeUpdate(sql2);
		stmt.executeUpdate(sql4);
		stmt.executeUpdate(sql5);
		System.out.println("dddddddddd");
		updateLogs();
		System.out.println("llllllllllllll");
		connect.connection.close();
		

	}

}
