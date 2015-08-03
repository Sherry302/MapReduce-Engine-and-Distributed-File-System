package edu.cmu.ds.p3.slave;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import edu.cmu.ds.p3.configuration.Config;

/**
 * listens for network communication in and deals with it appropriately.
 */
public class SlaveServer extends Thread {
	private Slave worker;
	private volatile boolean running;
	private ServerSocket sSock;

	public SlaveServer(Slave worker, Config config, String workerID) {
		this.running = true;
		this.worker = worker;
		try {
			this.sSock = new ServerSocket(config.getSlavePort(workerID));
		} catch (IOException e) {
			System.err.println("Unable to open Server Socket");
			e.printStackTrace();
		}
	}

	/**
	 * stop the socket listener
	 */
	public void stopWorker() {
		running = false;
		try {
			sSock.close();
		} catch (IOException e) {
			System.err.println("Error closing socket in stopWorker");
			e.printStackTrace();
		}
	}

	public void run() {
		while (running) {
			Socket s;
			try {
				s = sSock.accept();
			} catch (Exception e) {
				continue;
			}
			SlaveCommandHandler handler = new SlaveCommandHandler(worker, s);
			handler.start();
		}
	}
}
