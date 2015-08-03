package edu.cmu.ds.p3.util;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * The Message passed command
 */
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	public static enum TYPE {
		REGISTER, SUBMIT, START, STOP, MONITOR, STATUS, MAPPER, REDUCER, RESULT, EXCEPTION,
	}

	private TYPE type;
	private Task task = null;
	private String msg = null;
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	private String id = null;

	/**
	 * Send a Message to a host and return the response Message
	 */
	public static Message sendRequest(String remoteIp, int remotePort,
			int timeout, Message requestMsg) throws UnknownHostException,
			IOException, ClassNotFoundException, SocketTimeoutException {
		Socket soc = new Socket(remoteIp, remotePort);
		soc.setSoTimeout(timeout);
		ObjectOutputStream os = new ObjectOutputStream(soc.getOutputStream());
		os.writeObject(requestMsg);
		os.flush();
		ObjectInputStream is = new ObjectInputStream(soc.getInputStream());
		Message responseMsg = (Message) is.readObject();
		os.close();
		is.close();
		soc.close();
		return responseMsg;
	}

	public Message(TYPE type) {
		this.type = type;
	}

	public TYPE getType() {
		return type;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public void setType(TYPE type) {
		this.type = type;
	}

}
