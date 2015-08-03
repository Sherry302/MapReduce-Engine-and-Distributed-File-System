package edu.cmu.ds.p3.util;

import java.util.Map;

import edu.cmu.ds.p3.Interface.Mapper;

/**
 * MapTask holds all the info for a map task
 */
public class MapTask extends Task {

	private static final long serialVersionUID = 1L;
	private String inputFile;
	private InputSplit split;
	private Mapper mTask;
	private int mapperID;
	private Map<Integer, String> tmpResults;
	private String outputFolder;	
	
	public String getInputFile() {
		return inputFile;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public InputSplit getSplit() {
		return split;
	}

	public void setSplit(InputSplit split) {
		this.split = split;
	}	
	
	public Mapper getMTask() {
		return mTask;
	}

	public void setMTask(Mapper mTask) {
		this.mTask = mTask;
	}

	public int getMapperID() {
		return mapperID;
	}

	public void setMapperID(int mapperID) {
		this.mapperID = mapperID;
	}

	public Map<Integer, String> getTmpResults() {
		return tmpResults;
	}

	public void setTmpResults(Map<Integer, String> tmpResults) {
		this.tmpResults = tmpResults;
	}
	
	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}
}
