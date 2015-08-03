package edu.cmu.ds.p3.master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.cmu.ds.p3.util.Message;

/**
 * handle incoming message
 */
public class MasterCommandHandler extends Thread {
	private Master master;
	private Socket s;

	public MasterCommandHandler(Master master, Socket s) {
		this.master = master;
		this.s = s;
	}

	public void run() {
		ObjectInputStream is;
		ObjectOutputStream os;
		Message msg;

		try {
			os = new ObjectOutputStream(s.getOutputStream());
			is = new ObjectInputStream(s.getInputStream());
		} catch (Exception e) {
			System.err.println("error getting I/O streams");
			try {
				s.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			return;
		}		

		try {
			msg = (Message) is.readObject();
		} catch (Exception e) {
			System.err.println("Error receiving msg");			
			try {
				s.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}

		switch (msg.getType()) {
		case REGISTER:
			// add new worker to the worker table and reply back
			master.register(msg.getMsg());
			break;

		case SUBMIT:
			// submit a task
			if (!master.isAlive()) {
				msg.setMsg("Slave has not been started yet.");
			} else {
				master.submit(msg, s);
			}
			break;

		case START:
			// start the facility
			if (msg.getMsg().equals("START")) {
				msg.setMsg("Start Successfully");
				master.start();
			}
			break;

		case STOP:
			// don't accept any new task
			if (msg.getMsg().equals("STOP")) {
				msg.setMsg("Stop Successfully");
				try {
					master.shutdown();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			break;

		case MONITOR:
			// return the number of processes running on the worker
			if (!master.isAlive()) {
				msg.setMsg("The Map-Reduce facility has not been started yet.");
			} else {
				master.monitor(msg);
			}
			break;

		case MAPPER:
			// finish the map task and send out the reduce tasks
			try {
				master.mapper(msg);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			break;

		case REDUCER:
			// finish the reduce task and send back the result to the issuer
			master.reducer(msg);
			break;

		case EXCEPTION:
			// worker encounters a problem
			try {
				master.exception(msg);
			} catch (ClassNotFoundException | IOException e1) {
				e1.printStackTrace();
			}

		default:
			break;
		}
		try {
			os.writeObject(msg);
			os.flush();
			os.close();
			s.close();
		} catch (IOException e) {
			System.err.println("Error replying a new Job message");
			e.printStackTrace();
		}
		return;
	}
}
