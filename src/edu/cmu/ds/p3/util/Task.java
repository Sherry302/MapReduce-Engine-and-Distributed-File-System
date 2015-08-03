package edu.cmu.ds.p3.util;

import java.io.Serializable;

/**
 * A generic abstract class for all tasks
 */
public abstract class Task implements Serializable {

	private static final long serialVersionUID = 1L;
	private long taskID;
	private String slaveID;
	private String issuer;
	private String mapperClass;
	private String reducerClass;

	public String getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(String mapperClass) {
		this.mapperClass = mapperClass;
	}

	public String getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(String reducerClass) {
		this.reducerClass = reducerClass;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	public String getSlaveID() {
		return slaveID;
	}

	public void setSlaveID(String slaveID) {
		this.slaveID = slaveID;
	}

	public String getHandler() {
		return issuer;
	}

	public void setHandler(String issuer) {
		this.issuer = issuer;
	}

}
