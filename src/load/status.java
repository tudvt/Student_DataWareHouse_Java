package data_warehouse;
import java.sql.SQLException;
import java.sql.Statement;
public class status {
	
	void loadToData(String jdbcURL, String username, String password) throws SQLException {
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		Statement stmt = connect.connection.createStatement();
		String sql="use DW";
		stmt.executeUpdate(sql);
		String updateLog = "UPDATE log SET  log.statusend ='ER' where log.idlog like '1'";
		stmt.executeUpdate(updateLog);
	
	}
	public static void main(String[] args) throws SQLException{
		String jdbcURL = "jdbc:sqlserver://localhost";
		String username = "sa";
		String password = "sa";
		status st = new status();
		st.loadToData(jdbcURL, username, password);
	}
}
