import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.PreparedStatement;

public class ConnectDB {
	PreparedStatement statement = null;
	Connection connection = null;
	void connectDatabase(String jdbcURL,String username ,String password) throws SQLException {
		// TODO Auto-generated method stub
	
		
	
	//	String connectionURL = "jdbc:mysql://localhost:3306/datacontrol?rewriteBatchedStatements=true&relaxAutoCommit=true";
		connection = DriverManager.getConnection(jdbcURL, username, password);
		System.out.println("okkkkkkkkkk");
	}
	public static void main(String[] args) throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306";
		String username = "root";
		String password = "123456";
		ConnectDB cn= new ConnectDB();
		cn.connectDatabase(jdbcURL, username, password);
	}
}
