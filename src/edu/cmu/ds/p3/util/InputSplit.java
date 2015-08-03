package edu.cmu.ds.p3.util;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * partition the original file by size of mappers
 */
public class InputSplit implements Serializable {

	private static final long serialVersionUID = 1L;
	private String path;
	private long length;
	private long start;
	private int mapNum;

	@SuppressWarnings("resource")
	public ArrayList<InputSplit> getSplits() throws IOException {

		RandomAccessFile file = new RandomAccessFile(path, "r");;
		ArrayList<InputSplit> splitsMap = new ArrayList<InputSplit>();
		long fileSize = file.length();
		long splitSize = fileSize / (mapNum);
		long currStart = 0;
		long cnt = 0;

		while (cnt < mapNum) {
			this.start = currStart;
			this.length = splitSize;
			if (cnt != mapNum - 1) {
				splitsMap.add(new InputSplit(path, length, this.start));
				fileSize -= length;
			} else {
				splitsMap.add(new InputSplit(path, fileSize, this.start));
			}
			currStart = currStart + length;
			cnt++;
		}
		return splitsMap;
	}
	
	public ArrayList<String> getSplitedStrings() throws IOException {

		RandomAccessFile file;
		ArrayList<String> splitStrings = new ArrayList<String>();
		file = new RandomAccessFile(path, "r");
		long FileSize = file.length();
		long SplitSize = FileSize / (mapNum);
		
		long current_start = 0;
		long count = 0;

		while (count < mapNum) {
			this.start = current_start;
			this.length = SplitSize;
			if (count != mapNum - 1) {		
				byte[] buffer = new byte[(int)length];
				file.readFully(buffer, (int)this.start, (int)length);
				String fb = new String(buffer);
				splitStrings.add(fb);				
				FileSize -= length;
			} else {
				byte[] buffer = new byte[(int)FileSize];
				file.readFully(buffer, 0, (int)FileSize);
				String fb = new String(buffer);
				splitStrings.add(fb);
			}
			current_start = current_start + length;
			count++;
		}
		return splitStrings;
	}

	public InputSplit(String path, int mapNum) {
		this.path = path;
		this.mapNum = mapNum;
	}

	public InputSplit(String path, long length, long start) {
		this.path = path;
		this.length = length;
		this.start = start;
	}

	public String getPath() {
		return this.path;
	}

	public long getLength() {
		return this.length;
	}

	public long getStart() {
		return this.start;
	}
	
	public String setpath(String path) {
		return this.path = path;
	}
}
