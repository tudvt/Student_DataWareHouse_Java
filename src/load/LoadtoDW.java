package load;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.mysql.jdbc.PreparedStatement;

public class LoadtoDW {

	static String start, stop = null;

	private static void getFileFromStagging(String jdbcURL, String username, String password) throws SQLException {
		// timeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		start = dtf.format(now);
		System.out.println(start);

		// connect DB
		ConnectDB connect = new ConnectDB();
		connect.connectDatabase(jdbcURL, username, password);
		// create query
		Statement stmt = connect.connection.createStatement();
		// not fixed
		// Preparing a CallableStatement to call the retrieveData procedure
		// CallableStatement cstmt = connect.connection.prepareCall("{call
		// sampleProcedure()}");
		// Statement stmt2 = connect.connection.createStatement();
		// Statement stmt3 = connect.connection.createStatement();
		String errorVN = "SET SQL_SAFE_UPDATES = 0";
		stmt.executeUpdate(errorVN);
		System.out.println("turn off safe mode");

		// insert data to Store
		String insertStore = "insert sinhvien.datastore select * from sinhvien.stagging ";
		stmt.executeUpdate(insertStore);
		System.out.println("insert data from stagging to datastore");
		String useDB = "USE sinhvien";
		stmt.executeUpdate(useDB);
		System.out.println("select DB sinhvien");
		String createTemp = "create table temp as SELECT * FROM (SELECT * FROM sinhvien UNION ALL SELECT * FROM stagging) tbl GROUP BY stt HAVING count(*) = 1 ORDER BY stt";
		stmt.executeUpdate(createTemp);
		String check = "select count(*)  from temp";
		System.out.println("create table temp");
		System.out.println("check equal 2 DB");
		ResultSet rs = stmt.executeQuery(check);
		int count = 0;
		while (rs.next()) {
			count = rs.getInt(1);

		}
		System.out.println(count);
		// 2 DB not equal
		if (count > 0) {
			System.out.println("2 DB not equal");
			String stt = null;
			String error = "select stt from temp";
			rs = stmt.executeQuery(error);
			while (rs.next()) {
				stt = rs.getString("stt");
				System.out.println(stt);
				System.out.println("show error line");
			}
			String deleteStore = "DELETE FROM sinhvien.datastore";
			stmt.executeUpdate(deleteStore);
			System.out.println("delete datastore");
			String deleteTemp = "truncate table temp";
			stmt.executeUpdate(deleteTemp);
			System.out.println("delete temp");
			// DB are same
		} else {
			System.out.println("DB are same");
			System.out.println("inser store to ware house");
			String insertDW = "insert sinhvien.datawarehouse select * from sinhvien.datastore";
			stmt.executeUpdate(insertDW);
			String updateLog = "UPDATE datacontrol.log SET  log.statusend ='SU' where log.idlog like '1'";
			stmt.executeUpdate(updateLog);
			System.out.println("update status >>>>>>>");

			// update number of line start
			// System.out.println("update number of line start");
			// String numStartsql = "select count(*) from stagging";
			// int numStart = 0;
			// ResultSet rs2 = cstmt.getResultSet();;
			// rs2 = stmt2.executeQuery(numStartsql);
			//
			// while (!rs2.next()) {
			// numStart = rs.getInt(1);
			// String updateLogNumStart = "UPDATE datacontrol.log SET
			// log.numstart ='" + numStart + "' where log.idlog like '1'";
			// stmt.executeUpdate(updateLogNumStart);
			// System.out.println("update ok");
			// }
			// // // update number of line end
			// System.out.println("update number of line end");
			// String numENDsql = "select count(*) from datawarehouse";
			//
			// int numEND = 0;
			// ResultSet rs3 = cstmt.executeQuery(numENDsql);
			// while (!rs3.next()) {
			// numEND = rs.getInt(1);
			// String updateLogNumEnd = "UPDATE datacontrol.log SET log.numend
			// ='" + numEND + "' where log.idlog like '1'";
			// stmt.executeUpdate(updateLogNumEnd);
			// rs.close();
			//
			// }
		}
		String updateLogTimeStart = "UPDATE datacontrol.log SET  log.timestart ='" + start
				+ "' where log.idlog like '1'";
		stmt.executeUpdate(updateLogTimeStart);

		LocalDateTime now1 = LocalDateTime.now();
		stop = dtf.format(now1);
		String updateLogTimeEnd = "UPDATE datacontrol.log SET  log.timeend ='" + stop + "' where log.idlog like '1'";
		stmt.executeUpdate(updateLogTimeEnd);
		System.out.println("update time >>>>>>>");
		System.out.println(stop);
	}

	public static void main(String[] args) throws SQLException {
		System.out.println("chao mung cac ban den voi trai ngiem streamerrrrrrrrrrrr");
		String jdbcURL = "jdbc:mysql://localhost:3306";
		String username = "root";
		String password = "123456";
		LoadtoDW load = new LoadtoDW();
		load.getFileFromStagging(jdbcURL, username, password);
	}
}