package load;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class test {
	private static PreparedStatement pr = null;
	private static ResultSet rs = null;

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
//phuong thuc load file
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
	
// phuong thuc lay du lieu cua server va load file
	public static boolean downloadFile() {
		//tao bien tamp de kiem tra load thanh cong hay k
		boolean tamp = false;
		//cau lenh sql lay du lieu tu bang confgig
		String sql = "SELECT * FROM config";
		try {
			// connect voi db
			pr = new ConnectDB().getConnection().prepareStatement(sql);
			rs = pr.executeQuery();
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

		} catch (Exception e) {
			e.printStackTrace();
			return tamp;
		}
		return tamp;
	}
	
//phuong thuc ghi vao bang log
	public static boolean log() {
		int rs = 0;
		//tao bien tamp de kiem tra ghi vao db thanh cong hay khong
		boolean tamp = false;
		
		//cau sql ghi vao bang log trong db
		String sql = "INSERT INTO log (name,status,typeFile,path)" + "values(?,?,?,?);";
		File dir = new File("D:\\DATA_WH\\text");
		//tao danh sach cac file vua tai ve
		File[] file = dir.listFiles();
		for (int i = 0; i < file.length; i++) {
			try {
				//connect voi db
				pr = new ConnectDB().getConnection().prepareStatement(sql);
				//ghi vao cot ten file
				pr.setString(1, file[i].getName());
				//ghi vao cot trang thai
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
				//ghi vao cot duong dan
				pr.setString(4, file[i].getAbsolutePath());

				rs = pr.executeUpdate();
				//ghi thanh cong thi gan tamp=true
				tamp = true;
			} catch (Exception e) {
				e.printStackTrace();
				return tamp;
			}
		}
		return tamp;
	}

	//phuong thuc gui mai
	public static boolean mail( String subject, String bodyMail) {
		Properties props = new Properties();
		// cau hinh mail server
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
		try {
			MimeMessage message = new MimeMessage(session);
			message.setHeader("Content-Type", "text/plain; charset=UTF-8");
			//dia chi mail gui di
			message.setFrom(new InternetAddress("lle718817@gmail.com"));
			//dia chi mai gui den
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("lxl2808@gmail.com"));
			//tieu de mail
			message.setSubject(subject, "UTF-8");
			//noi dung mail
			message.setText(bodyMail, "UTF-8");
			Transport.send(message);
		} catch (MessagingException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void sendMail() {
		//neu log tra ve false thi gui mail bao khong thanh cong
		if (log() != true) {
			mail("data warehouses 2020", "ghi vao log khong thanh cong ");
			System.out.println("khong thanh cong");
		}
		//neu log tra ve true thi gui mail bao thanh cong
		else {
			mail("data warehouses 2020", "ghi vao log thanh cong");
			System.out.println("thanh cong");
		}
	}

	public static void main(String argv[]) {
		downloadFile();
		sendMail();
	}

}
