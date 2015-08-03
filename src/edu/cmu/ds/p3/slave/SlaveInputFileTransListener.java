package edu.cmu.ds.p3.slave;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;

import edu.cmu.ds.p3.configuration.Config;

/**
 * mapper listens for master to transfer split input
 */
public class SlaveInputFileTransListener extends Thread {
	private Slave slave;
	private volatile boolean running;
	private ServerSocket sSock;
	private RandomAccessFile file;

	public SlaveInputFileTransListener(Slave slave, Config config, String workID) {
		this.running = true;
		this.slave = slave;
		try {
			int cport = config.getClientInputFileTranPort(workID);
			this.sSock = new ServerSocket(cport);			
			//System.out.println("the port is: " + workID + " " + cport);
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
				ObjectInputStream in = new ObjectInputStream(s.getInputStream());
				String sfile = (String) in.readObject();
				String path = slave.receivedFile;
				file = new RandomAccessFile(path, "rw");
				file.write(sfile.getBytes());
				in.close();
				s.close();
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
 
		}
	}
}
