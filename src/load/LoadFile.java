package data_warehouse;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import java.util.*;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class LoadFile {

	String condition;

	public void LoadFile(String condition) {
		this.condition = condition;
	}

	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static String scpDownload(String hostname, int port, String user, String pw, String remotePath,
			String localPath) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("hello ");
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "";
		}
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(user, pw);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "";
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "";
		}
		// down tat ca cac file bat dau bang "sinhvien"
		scp.put_SyncMustMatch("sinhvien_chieu*.*");
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "";
		}

		ssh.Disconnect();
		return "";

	}

	public static boolean log() {
		boolean check = false;

		try {
			// connect database
			String myDriver = "com.mysql.jdbc.Driver";
			String myUrl = "jdbc:mysql://localhost/wh";
			Class.forName(myDriver);
			Connection conn = DriverManager.getConnection(myUrl, "root", "");

			// insert to table log
			String sql = "INSERT INTO log (name,status,typeFile,path)" + "values(?,?,?,?);";
			File dir = new File("D:\\DATA_WH\\text");
			File[] children = dir.listFiles();
			for (File file : children) {
				PreparedStatement preparedStmt = conn.prepareStatement(sql);
				// file name
				preparedStmt.setString(1, file.getName());
				// status
				preparedStmt.setString(2, "ER");
				// type file
				if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".xlsx")) {
					preparedStmt.setString(3, "xlsx");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".txt")) {
					preparedStmt.setString(3, "txt");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".csv")) {
					preparedStmt.setString(3, "csv");
				} else if (file.getName().substring(file.getName().lastIndexOf(".")).equals(".osheet")) {
					preparedStmt.setString(3, "osheet");
				} else {
					preparedStmt.setString(3, "kb");
				}
				// file path
				preparedStmt.setString(4, file.getAbsolutePath());

				preparedStmt.executeUpdate();
				check = true;
			}
			conn.close();
		} catch (Exception e) {
			return check;
		}
		System.err.println("load to log success");
		return check;
	}

	public static boolean sendMail(String to, String subject, String bodyMail) {
		Properties props = new Properties();
		// Setup mail server
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");
		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("lle718817@gmail.com", "tes0335444964");
			}
		});
		// Used to debug SMTP issues
//		session.setDebug(true);
		try {
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);
			// Set From: header field of the header.
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			// Set To: header field of the header.
			message.setFrom(new InternetAddress("lle718817@gmail.com"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			// Set Subject: header field
			message.setSubject(subject, "UTF-8");
			// Now set the actual message
			message.setText(bodyMail, "UTF-8");
			// Send message
			Transport.send(message);
		} catch (MessagingException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void sendMailLog() {
		if (log() != true) {
			sendMail("17130113@st.hcmuaf.edu.vn", "DATA WAREHOUSE - 2020", "Download file khong thanh cong! ");
			System.out.println("Send Email- fail...!");
		} else {
			sendMail("17130113@st.hcmuaf.edu.vn", "DATA WAREHOUSE - 2020", "Downoad file thanh cong! ");
			System.out.println("Send Email- success...!");
		}
	}

	public static void main(String argv[]) {
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		String user = "guest_access";
		String pw = "123456";
		// souce
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		// thu muc muon down file ve
		String localPath = "D:\\DATA_WH\\text";
//		scpDownload(hostname, port, user, pw, remotePath, localPath);
		log();
		sendMailLog();
	}

}
