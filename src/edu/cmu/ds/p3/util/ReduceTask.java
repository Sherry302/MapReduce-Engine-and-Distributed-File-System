package edu.cmu.ds.p3.util;

import java.util.List;

import edu.cmu.ds.p3.Interface.Reducer;

public class ReduceTask extends Task {
	private static final long serialVersionUID = 1L;
	private int reducerID;
	private List<String> inputFiles;
	private Reducer rTask;
	private String outputFolder;

	public int getReducerID() {
		return reducerID;
	}

	public void setReducerID(int reducerID) {
		this.reducerID = reducerID;
	}

	public List<String> getInputFiles() {
		return inputFiles;
	}

	public void setInputFiles(List<String> inputFiles) {
		this.inputFiles = inputFiles;
	}

	public Reducer getRTask() {
		return rTask;
	}

	public void setRTask(Reducer rTask) {
		this.rTask = rTask;
	}

	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

}
