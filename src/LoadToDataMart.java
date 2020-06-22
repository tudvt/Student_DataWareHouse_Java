import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import com.mysql.jdbc.PreparedStatement;

public class LoadToDataMart {
	public static void load(String locationsrc, String locationdes) throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/datamart";
		String username = "root";
		String password = "123456";
		PreparedStatement statement = null;
		Connection connection = null;
		// String connectionURL =
		// "jdbc:mysql://localhost:3306/datamart?rewriteBatchedStatements=true&relaxAutoCommit=true";
		connection = DriverManager.getConnection(jdbcURL, username, password);
		System.out.println("ssssssss");
		// copy to another db(datamart)
		Statement stmt = connection.createStatement();

		String sql2 = "insert '" + locationsrc + "' " + "select * from '" + locationdes + "'";

		stmt.executeUpdate(sql2);

		System.out.println("dddddddddd");
		connection.close();
	}

	public static void main(String[] args) throws SQLException {
		System.out.println("dsd");
		String jdbcURL = "localhost";
		String username = "root";
		String password = "123456";
		PreparedStatement statement = null;
		Connection connection = null;
		String connectionURL = "jdbc:mysql://localhost:3306/datacontrol?rewriteBatchedStatements=true&relaxAutoCommit=true";
		connection = DriverManager.getConnection(connectionURL, username, password);
		// find file has status SU
		String sql = "select datacontrol.locationsrc,datacontrol.locationdes,datacontrol.numoflistsrc from datacontrol inner join log on datacontrol.ID =log.id where log.statusend like 'TR'";
		// time start
		long start = System.currentTimeMillis();
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();
		System.out.println(dtf.format(now));
		System.out.println(start);
		// create the java statement
		Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		connection.setAutoCommit(false);
		// execute the query, and get a java resultset
		stmt.addBatch(sql);

		ResultSet rs = stmt.executeQuery(sql);

		// iterate through the java resultset
		String locationsrc = "";
		String locationdes = "";
		String numoflistsrc = "";

		while (rs.next()) {
			locationsrc = rs.getString("datacontrol.locationsrc");
			locationdes = rs.getString("datacontrol.locationdes");
			numoflistsrc = rs.getString("datacontrol.numoflistsrc");

			// print the results
			System.out.format("%s, %s, %s\n", locationsrc, locationdes, numoflistsrc);

		}

		connection.commit();
		System.out.println("ssssssss");
		load(locationsrc, locationdes);
		// // copy to another db(datamart)
		// String sql3="insert '"+locationdes+"' select * from
		// '"+locationsrc+"'";
		// stmt.executeUpdate(sql3);
		// //String sql2 = "update sinhvien.hocsinh set sinhvien.hocsinh.Tên='f'
		// where sinhvien.hocsinh.STT like '1'";
		//
		//
		// // stmt.executeUpdate(sql2);
		// System.out.println("rưerwer");

		System.out.println("dddddddddd");
		// time end
		long end = System.currentTimeMillis();
		System.out.println(dtf.format(now));
		System.out.println(end);
		System.out.printf("Import done in %d ms\n", (end - start));

	}
}