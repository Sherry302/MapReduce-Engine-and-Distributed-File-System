package edu.cmu.ds.p3.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import edu.cmu.ds.p3.configuration.Config;

/**
 * listens for network communication in and deals with it appropriately.
 */
public class MasterServer extends Thread {
	private Master master;
	private volatile boolean isRunning;
	ServerSocket s;

	public MasterServer(Master master, Config config) {
		this.isRunning = true;
		this.master = master;
		try {
			this.s = new ServerSocket(config.getMasterPort());
		} catch (IOException e) {
			System.err.println("Open Server Socket Fail");
			e.printStackTrace();
		}
	}

	/**
	 * stop the socket listener
	 */
	public void stopServer() {
		isRunning = false;
		try {
			s.close();
		} catch (IOException e) {
			System.err.println("Stop Server Fail");
			e.printStackTrace();
		}
	}

	public void run() {
		while (isRunning) {
			Socket soc;
			try {
				soc = s.accept();
			} catch (Exception e) {
				continue;
			}
			MasterCommandHandler handler = new MasterCommandHandler(master, soc);
			handler.start();
		}
	}

}
