package src;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class load {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/datacontrol";
	// Ten nguoi dung va mat khau cua co so du lieu
	static final String USER = "root";
	static final String PASS = "";
	static Connection conn = null;
	static CallableStatement stmt = null;
	static ArrayList<String> listER = new ArrayList<String>();
	static Map<String, String> map = new HashMap<>();
	static SendEmail sendMail;
	private static String USER_NAME = "dotuongtu197@gmail.com"; // ng gui
	private static String PASSWORD = "kid159753"; // GMail password
	private static String RECIPIENT = "ngovianpanda123@gmail.com";// ng nhan
	static String subject = "Thong bao ";
	static String body = "load to stagging thanh cong";
	static String[] listEmail = { RECIPIENT };
	static String table_target = "";
	static String table_target1 = "";
	static String db_target = "";
	static String temp_target = "";
	static String db_config = "";

	static void connectDb() throws ClassNotFoundException, SQLException {

		// Buoc 2: Dang ky Driver
		Class.forName("com.mysql.jdbc.Driver");
		// Buoc 3: Mo mot ket noi
		System.out.println("Dang ket noi toi co so du lieu ...");
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		System.out.println("Tao cac lenh truy van SQL ...");
	}

	private static void getConfigData(int id) throws SQLException {
		// lay thong tin data trong table dataconfig
		String sql = "{call datacontrol.getAllDataconfig (?)}";
		stmt = conn.prepareCall(sql);
		int id_config = id;
		stmt.setInt(1, id_config);
		ResultSet rs = stmt.executeQuery();
		while (rs.next()) {
			db_target = rs.getString(12);
			System.err.println(db_target);
			table_target = rs.getString(13);
			temp_target = rs.getString(14);
			db_config = rs.getString(15);
		}
	}

	private void getFileFromServer() throws SQLException {

		// lay data ve
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("hello");
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw("guest_access", "123456");
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch("sinhvien*.*");// down tat ca cac file bat dau
												// bang sinhvien
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		String localPath = "C:\\Users\\Tuong Tu\\Desktop\\copy"; // thu muc muon
																	// down file
																	// ve
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		System.out.println("okkkkkkkkkkkkk");
		ssh.Disconnect();
	}

	static void getFileER(int id_config) throws ClassNotFoundException, SQLException {

		String sql = "{call datacontrol.getFile_local (?)}";
		stmt = conn.prepareCall(sql);
		// Dau tien gan ket tham so IN

		stmt.setInt(1, id_config);

		// Su dung phuong thuc execute de chay stored procedure.
		System.out.println("Thuc thi stored procedure ...");
		ResultSet rs = stmt.executeQuery();
		String file_src = "";
		while (rs.next()) {
			// gan gia tri key value bang idlog va file name log
			String key = rs.getString(1);
			String value = rs.getString(2);
			map.put(key, value);
			System.out.println(key + "  " + value);

		}
	}

	static void loadToStagging() throws SQLException {
		String useDB = "use " + db_target;
		System.out.println(useDB);

		System.out.println(table_target);
		stmt.executeUpdate(useDB);
		System.err.println("Bat dau load");
		for (Map.Entry<String, String> entry : map.entrySet()) {
			String target = "sinhvien.stagging";
			String k = entry.getKey();
			String v = entry.getValue();
			System.out.println("Key: " + k + ", Value: " + v);

			// dung load file ko the bo vao procedure
			String load_stagging = "LOAD DATA  INFILE '" + v + "' " + "INTO TABLE " + target + ""
					+ " FIELDS TERMINATED BY '\t' " + "ENCLOSED BY '' " + "LINES TERMINATED BY '\r\n';";

			System.out.println("Dang load dong:  " + v);
			stmt.executeUpdate(load_stagging);
			System.out.println("load ok");
		}
		
	static void setStatusTR() throws SQLException{
		// set ER -->>> TR
				String useDB1 = "use " + db_config;
				System.out.println(useDB1);
				System.out.println(table_target1);
				stmt.executeUpdate(useDB1);
				
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String k1 = entry.getKey();
					String v1 = entry.getValue();
					System.out.println("Key: " + k1 + ", Value: " + v1);

					// set ER sang TR
					String set_statusTR = "UPDATE log SET statusend =\"TR\" WHERE filenamesrc=\""+ v1 +"\"";
					stmt.executeUpdate(set_statusTR);
					System.out.println("Set ER thanh TR ok");				
	} 
		
		
		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
		System.err.println("gui email ok");
	}

	private static void loadToTemp() throws SQLException {
		String use_dc = "use " + db_target;
		stmt.executeUpdate(use_dc);
		String call_insert = " insert into " + temp_target + " select * from " + table_target + "";
		stmt = conn.prepareCall(call_insert);
		stmt.executeUpdate();
		System.err.println("load from stagging to temp");

		System.out.println("dung db sinh vien");
		String call_truncate = " TRUNCATE TABLE " + table_target + ";";
		stmt = conn.prepareCall(call_truncate);
		System.out.println("xoa du lieu trong stagging");
		stmt.executeUpdate();
		System.err.println("load from stagging to temp");

	}

	private static void editTemp() throws SQLException {
		String use_dc = "use " + db_target;
		stmt.executeUpdate(use_dc);
		// cai cu cac nay de chay duoc update
		String setSafeMode = "SET SQL_SAFE_UPDATES = 0;";
		stmt.executeUpdate(use_dc);
		// xoa cmn dong stt
		String deleteStt = "delete from  " + temp_target + " where stt='stt'";
		stmt.executeUpdate(deleteStt);
		System.out.println("xoa dong stt");
		// them sk vao ne
		String alter_temp = " ALTER TABLE " + temp_target + " ADD sk INT PRIMARY KEY AUTO_INCREMENT;";
		stmt = conn.prepareCall(alter_temp);
		stmt.executeUpdate();
		System.out.println("tao khoa sk cho temp");
		// tao 1 thoi gian la thoi gian hien tai luc chay
		String addDate_temp = "alter table " + temp_target + " add date_temp date;";
		stmt = conn.prepareCall(addDate_temp);
		stmt.executeUpdate();
		System.out.println("them cot date_temp");
		// day la date sk láº¥y tá»« date dim
		String addDate_sk = "alter table " + temp_target + " add date_sk int;";
		stmt = conn.prepareCall(addDate_sk);
		stmt.executeUpdate();
		System.out.println("them cot date_sk");
		// day la date lastchange
		String addDate_lastchange = "alter table " + temp_target + " add date_lastchange date;";
		stmt = conn.prepareCall(addDate_lastchange);
		stmt.executeUpdate();
		System.out.println("them cot date_lastchange");
		// set gia tri cot lÃ  ngay hien tai
		String add_curdate = "UPDATE " + temp_target + " SET date_temp = curdate(); ";
		stmt = conn.prepareCall(add_curdate);
		stmt.executeUpdate();
		System.out.println("dat gia tri ngay chay chuong trinh");
		// update date sk cho table
		String update_date_sk = "UPDATE " + temp_target
				+ " SET date_sk = (select date_sk from date_dim where temp.date_temp = date_dim.full_date);";
		stmt = conn.prepareCall(update_date_sk);
		stmt.executeUpdate();
		System.out.println("update date sk cho table");

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {


		connectDb();
		getConfigData(1);
		getFileER(1);
		loadToStagging();
		setStatusTR();
		// loadToTemp();
		// editTemp();

	}
}
