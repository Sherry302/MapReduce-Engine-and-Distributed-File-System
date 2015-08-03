package edu.cmu.ds.p3.slave;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import edu.cmu.ds.p3.configuration.Config;

/**
 * Listening if reducers ask mappers for intermediate results 
 */
public class SlaveInterResTransListener extends Thread {
	private Slave slave;
	private volatile boolean running;
	private ServerSocket sSock;
	
	public SlaveInterResTransListener(Slave slave, Config config, String workID) {
		this.running = true;
		this.slave = slave;

		try {
			int iport = config.getClientInterResTranPort(workID);
			this.sSock = new ServerSocket(iport);
		} catch (IOException e) {
			System.err.println("Unable to open Server Socket");
			e.printStackTrace();
		}
	}

	/**
	 * stop the socket listener
	 */
	public void stopSlave() {
		running = false;
		try {
			sSock.close();
		} catch (IOException e) {
			System.err.println("Error closing socket in stopSlave");
			e.printStackTrace();
		}
	}

	public void run() {
		while (running) {
			Socket s;
			try {
				s = sSock.accept();

				while (!slave.isInterTransReady)
					;

				List<String> paths = slave.getPartitionPaths();
				int pathnum = 1;
				for (int i = 0; i < pathnum; i++) {
					RandomAccessFile file = new RandomAccessFile(paths.get(i), "rw");
					long FileSize = file.length();
					byte[] buffer = new byte[(int) FileSize];
					file.readFully(buffer, 0, (int) FileSize);
					String fb = new String(buffer);

					ObjectOutputStream out = new ObjectOutputStream(
							s.getOutputStream());
					out.writeObject(fb);
					out.flush();
					out.close();
					s.close();
					file.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}

		}
	}
}
