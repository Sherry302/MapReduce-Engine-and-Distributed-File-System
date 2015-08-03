package edu.cmu.ds.p3.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Emitting key value pairs to an intermediary results file
 */
public class TmpKV2TmpResult {
	private String tmpDir;
	private int tmpFileNum = 1;
	private int bufferSize;
	private List<String> splitPaths = new ArrayList<String>();
	private List<TmpKVPair> buffer = new LinkedList<TmpKVPair>();

	/**
	 * create temp file path
	 * 
	 * @param tmpFileNum
	 * @return temp split filepath
	 */
	private String getSplitFilePath(int tmpFileNum) {
		return this.tmpDir + File.separator + "split_" + tmpFileNum;
	}

	/**
	 * Collect the result and emit to the split file
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public void collect(String key, String value) throws IOException {
		this.buffer.add(new TmpKVPair(key, value));
		if (this.buffer.size() >= this.bufferSize)
			emit();
	}

	/**
	 * emit the result to intermediate file
	 * 
	 * @throws IOException
	 */
	public void emit() throws IOException {
		Collections.sort(buffer);
		String splitPath = this.getSplitFilePath(tmpFileNum);
		ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(
				splitPath));
		for (TmpKVPair pair : buffer) {
			os.writeObject(pair);
			os.flush();
		}
		os.close();
		buffer.clear();
		this.splitPaths.add(splitPath);
		this.tmpFileNum++;
	}

	public String getTmpDir() {
		return tmpDir;
	}

	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}

	public int getTmpFileNum() {
		return tmpFileNum;
	}

	public void setTmpFileNum(int tmpFileNum) {
		this.tmpFileNum = tmpFileNum;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public List<String> getSplitPaths() {
		return splitPaths;
	}

	public void setSplitPaths(List<String> splitPaths) {
		this.splitPaths = splitPaths;
	}

	public List<TmpKVPair> getBuffer() {
		return buffer;
	}

	public void setBuffer(List<TmpKVPair> buffer) {
		this.buffer = buffer;
	}
}
