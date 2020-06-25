package pscp;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class LoadFile {
	static {
		try {
			System.loadLibrary("chilkat"); 
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void main(String argv[])  {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("hello ");
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
		// down tat ca cac file bat dau bang "sinhvien"
		scp.put_SyncMustMatch("sinhvien*.*");
		//souce
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		// thu muc muon down file ve
		String localPath = "D:\\DATA_WH\\text"; 
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}

		ssh.Disconnect();
	}
	
}
