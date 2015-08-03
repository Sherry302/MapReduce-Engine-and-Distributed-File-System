package edu.cmu.ds.p3.slave;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.cmu.ds.p3.util.Message;

/**
 * handle incoming message
 */
public class SlaveCommandHandler extends Thread {
	private Slave slave;
	private Socket s;

	public SlaveCommandHandler(Slave slave, Socket s) {
		this.slave = slave;
		this.s = s;
	}

	public void run() {
		ObjectInputStream is;
		ObjectOutputStream os;
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

		Message msg;

		try {
			msg = (Message) is.readObject();
		} catch (Exception e) {
			System.err.println("Error receiving msg");
			try {
				s.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			return;
		}

		switch (msg.getType()) {
		case STATUS:
			// check status
			if (msg.getMsg().equals("QUERY"))
				msg.setMsg("HEALTHY");
			break;

		case MAPPER:
			// start a map task
			slave.map(msg);
			break;

		case REDUCER:
			// start a reduce task
			try {
				slave.reduce(msg);
			} catch (InterruptedException e1) {

				//e1.printStackTrace();
			}
			break;

		case RESULT:
			// print the result
			System.out.println(msg.getMsg());

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
