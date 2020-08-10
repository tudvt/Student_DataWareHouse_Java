import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
	static final String DB_URL = "jdbc:mysql://localhost:3306/datacontrol";
static PreparedStatement pr=null;
	// Ten nguoi dung va mat khau cua co so du lieu
	static final String USER = "root";
	static final String PASS = "123456";
	static Connection conn = null;
	static CallableStatement stmt = null;
	static ArrayList<String> listER = new ArrayList<String>();
	static Map<String, String> map = new HashMap<>();
	static SendEmail sendMail;
	private static String USER_NAME = "dotuongtu197@gmail.com"; // GMail user
																// name (just
																// the part
																// before
																// "@gmail.com")
	private static String PASSWORD = "kid159753"; // GMail password
	private static String RECIPIENT = "dotuongtu198@gmail.com";
	static String subject = "Thong bao ";
	static String body = " thanh cong";
	static String[] listEmail = { RECIPIENT };
	static String table_target = "";
	static String db_target = "";
	static String temp_target = "";
	static String db_config = "";
	static String table_end = "";

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
			System.out.println(db_target);
			table_target = rs.getString(13);// table stagiing
			temp_target = rs.getString(14);// table temp
			db_config = rs.getString(15);
			table_end = rs.getString(16);// table sau khi da edit temp
		}

	}

	public static boolean log() {
		int rs = 0;
		// tao bien tamp de kiem tra ghi vao db thanh cong hay khong
		boolean tamp = false;
		PreparedStatement pr = null;
		// cau sql ghi vao bang log trong db
		String sql = "INSERT INTO log(filenamesrc,statusend,typeFile,idconfig)" + "values(?,?,?,?);";
		File dir = new File("C:\\Users\\Tuong Tu\\Desktop\\local");
		// tao danh sach cac file vua tai ve
		File[] file = dir.listFiles();
		for (int i = 0; i < file.length; i++) {
			try {
				// connect voi db
				pr = conn.prepareCall(sql);
				if (file[i].getName().toLowerCase().startsWith("sinhvien")) {
					pr.setInt(4, 1);
				} else {
					pr.setInt(4, 1);
				}

				// ghi vao cot duong dan
				pr.setString(1, file[i].getAbsolutePath());
				// ghi vao cot trang thai
				pr.setString(2, "ER");
				// loai file
				if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".xlsx")) {
					pr.setString(3, "xlsx");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".txt")) {
					pr.setString(3, "txt");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".csv")) {
					pr.setString(3, "csv");
				} else if (file[i].getName().substring(file[i].getName().lastIndexOf(".")).equals(".osheet")) {
					pr.setString(3, "osheet");
				} else {
					pr.setString(3, "kb");
				}

				rs = pr.executeUpdate();

				// ghi thanh cong thi gan tamp=true
				tamp = true;
			} catch (Exception e) {
				e.printStackTrace();
				return tamp;
			}
		}
		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
		return tamp;
	}

//	private void getFileFromServer() throws SQLException {
//
//		// lay data ve
//		CkSsh ssh = new CkSsh();
//		CkGlobal ck = new CkGlobal();
//		ck.UnlockBundle("hello");
//		String hostname = "drive.ecepvn.org";
//		int port = 2227;
//		boolean success = ssh.Connect(hostname, port);
//		if (success != true) {
//			System.out.println(ssh.lastErrorText());
//			return;
//		}
//
//		ssh.put_IdleTimeoutMs(5000);
//		success = ssh.AuthenticatePw("guest_access", "123456");
//		if (success != true) {
//			System.out.println(ssh.lastErrorText());
//			return;
//		}
//		CkScp scp = new CkScp();
//
//		success = scp.UseSsh(ssh);
//		if (success != true) {
//			System.out.println(scp.lastErrorText());
//			return;
//		}
//		scp.put_SyncMustMatch("sinhvien*.*");// down tat ca cac file bat dau
//												// bang sinhvien
//		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
//		String localPath = "C:\\Users\\Tuong Tu\\Desktop\\copy"; // thu muc muon
//																	// down file
//																	// ve
//		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
//		if (success != true) {
//			System.out.println(scp.lastErrorText());
//			return;
//		}
//		System.out.println("okkkkkkkkkkkkk");
//		ssh.Disconnect();
//	}
	public static void scpDownload(String hostname, int port, String user, String pw, String remotePath,
			String localPath, String syn_must_math) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("hello ");
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
		}
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(user, pw);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
		}
		CkScp scp = new CkScp();
		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
		}
		scp.put_SyncMustMatch(syn_must_math);
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
		}
		ssh.Disconnect();
	}
	public static boolean downloadFile() {
		//tao bien tamp de kiem tra load thanh cong hay k
		boolean tamp = false;
		//cau lenh sql lay du lieu tu bang confgig
		String sql = "SELECT * FROM config";
		try {
			ResultSet rs= null;
			// connect voi db
			stmt = conn.prepareCall(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				// host name
				String hostname = rs.getString("hostname");
				//port
				int port = rs.getInt("port");
				//user name
				String username = rs.getString("username");
				//password
				String pw = rs.getString("password");
				//down tat ca cac file bat dau bang...
				String syn_must_math = rs.getString("syn_must_math");
				//duong dan cua server
				String server_path = rs.getString("server_path");
				//duong dan cua local
				String local_path = rs.getString("local_path");
				//phuong thuc load file
				scpDownload(hostname, port, username, pw, server_path, local_path, syn_must_math);
				//load thanh cong thi gan tamp=true
				tamp = true;
				return tamp;
			}
			pr.close();
			rs.close();
		} catch (Exception e) {
			e.printStackTrace();
			return tamp;
		}
		return tamp;
	}
	
	static void getFileER(int id_config) throws ClassNotFoundException, SQLException {

//		String replace="UPDATE  log "
//				+ "SET  filenamesrc = REPLACE (filenamesrc, '\\' , '/');";
//		stmt = conn.prepareCall(replace);
//		stmt.executeUpdate();
		String replace = "{call datacontrol.replace()}";
		stmt = conn.prepareCall(replace);
		stmt.executeUpdate();
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
		// su dung db sinh vien
		String useDB = "use " + db_target;
		System.out.println(useDB);

		System.out.println(table_target);
		stmt.executeUpdate(useDB);
		System.out.println("Bat dau load");
		// duyet cai map lay ra các file ER o phan tren
		for (Map.Entry<String, String> entry : map.entrySet()) {
			String target = "sinhvien.stagging";
			String k = entry.getKey();
			String v = entry.getValue();

			System.out.println("Key: " + k + ", Value: " + v);


			// load file vao db
			// dung load file ko the bo vao procedure
			String load_stagging = "LOAD DATA  INFILE '" + v + "' " + "INTO TABLE " + target + ""
					+ " FIELDS TERMINATED BY '\t' " + "ENCLOSED BY '' " + "LINES TERMINATED BY '\r\n';";

			System.out.println("Dang load dong:  " + v);
			stmt.executeUpdate(load_stagging);
			System.out.println("load ok");
			String sql_filename = "";

		}
		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
		System.out.println("gui email ok");
	}

	static void setStatusTR() throws SQLException {
		// set ER -->>> TR
		String useDB1 = "use " + db_config;
		System.out.println(useDB1);
		stmt.executeUpdate(useDB1);

		for (Map.Entry<String, String> entry : map.entrySet()) {
			String k1 = entry.getKey();
			String v1 = entry.getValue();
			System.out.println("Key: " + k1 + ", Value: " + v1);

			// set ER sang TR
			String set_statusTR = "UPDATE log SET statusend = \"TR\" WHERE filenamesrc=\"" + v1 + "\"";
			stmt.executeUpdate(set_statusTR);
			System.out.println("Set ER thanh TR ok");
		}
		map.clear();
	}

	private static void loadToTemp() throws SQLException {
		String use_dc = "use " + db_target;
		stmt.executeUpdate(use_dc);
		// goi ham insert gai tri tu stagging vao temp
		String call_insert = " insert into " + temp_target + " select * from " + table_target + "";

		stmt = conn.prepareCall(call_insert);
		stmt.executeUpdate();
		System.out.println("load from stagging to temp");

		System.out.println("dung db sinh vien");
		String call_truncate = " TRUNCATE TABLE " + table_target + ";";
		stmt = conn.prepareCall(call_truncate);
		System.out.println("xoa du lieu trong stagging");
		stmt.executeUpdate();

		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
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
		String alter_temp = " ALTER TABLE " + temp_target + " ADD sk INT ";
		stmt = conn.prepareCall(alter_temp);
		stmt.executeUpdate();
		System.out.println("tao khoa sk cho temp");
		// tao 1 thoi gian la thoi gian hien tai luc chay
		String addDate_temp = "alter table " + temp_target + " add date_temp date;";
		stmt = conn.prepareCall(addDate_temp);
		stmt.executeUpdate();
		System.out.println("them cot date_temp");
		// day la date sk lấy từ date dim
		String addDate_sk = "alter table " + temp_target + " add date_sk int;";
		stmt = conn.prepareCall(addDate_sk);
		stmt.executeUpdate();
		System.out.println("them cot date_sk");
		// day la date lastchange
		String addDate_lastchange = "alter table " + temp_target + " add date_lastchange date;";
		stmt = conn.prepareCall(addDate_lastchange);
		stmt.executeUpdate();
		System.out.println("them cot date_lastchange");
		// set gia tri cot là ngay hien tai
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

		String table_end_sql = "insert into "+table_end+ " select * from  " + temp_target + ";";
		stmt = conn.prepareCall(table_end_sql);
		stmt.executeUpdate();
		System.out.println("ra table cuoi cung");
	//	 delete tat ca may cot them vao temp
		String delete_sk = "ALTER TABLE " + temp_target + " DROP COLUMN sk;";
		stmt = conn.prepareCall(delete_sk);
		stmt.execute();
//;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
		String delete_date_lastchange = "ALTER TABLE " + temp_target + " DROP COLUMN date_lastchange;";
		stmt = conn.prepareCall(delete_date_lastchange);
		stmt.execute();

		String delete_date_temp = "ALTER TABLE " + temp_target + " DROP COLUMN date_temp;";
		stmt = conn.prepareCall(delete_date_temp);
		stmt.execute();

		String delete_date_sk = "ALTER TABLE " + temp_target + " DROP COLUMN date_sk;";
		stmt = conn.prepareCall(delete_date_sk);
		stmt.execute();

		 String truncate_temp = "TRUNCATE TABLE " + temp_target + ";";
		 stmt = conn.prepareCall(truncate_temp);
		 stmt.executeUpdate();

		System.out.println("delete all new add col");

		sendMail = new SendEmail();
		sendMail.sendFromGMail(USER_NAME, PASSWORD, listEmail, subject, body);
		System.out.println("gui mail ok ");
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		connectDb();
		int id_config = Integer.parseInt(args[0]);

		getConfigData(id_config);
		log();
		getFileER(id_config);
		loadToStagging();
		setStatusTR();
		loadToTemp();
		editTemp();
		System.out.println("okkkkkk");

	}
}
