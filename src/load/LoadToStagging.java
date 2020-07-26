package load;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadToStagging {
	private void loadToStagging(String jdbcURL, String username, String password) throws SQLException {
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		Statement stmt = connect.connection.createStatement();
		String sql="select idLog,name from log where status='ER'";
		stmt.execute(sql);
		
		String sql1="use wh";
		stmt.executeUpdate(sql1);
		String sqlER="update log set status='TR' where idLog=''";
		stmt.executeUpdate(sqlER);		
		
	}

}
