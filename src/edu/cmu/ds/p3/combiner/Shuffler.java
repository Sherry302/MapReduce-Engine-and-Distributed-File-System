package edu.cmu.ds.p3.combiner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.cmu.ds.p3.util.*;

/**
 * partition sorted file by hashcode value of key
 */
public class Shuffler {

	private int bufferSize;
	private String tmpDir = null;
	private int partitionNum = 1;
	private Map<Integer, ObjectOutputStream> shuffleIDMap = new HashMap<Integer, ObjectOutputStream>();
	private List<String> partitionPaths = new ArrayList<String>();

	public int getPartitionNum() {
		return partitionNum;
	}

	public void setPartitionNum(int partitionNum) {
		this.partitionNum = partitionNum;
	}

	public String getTmpDir() {
		return tmpDir;
	}

	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}
	
	public List<String> getPartitionPaths() {
		return partitionPaths;
	}

	/**
	 * partition by hashcode
	 * 
	 * @param key
	 * @param value
	 * @param partitionsNum
	 * @return
	 */
	private int getPostShuffleID(String key, String value, int partitionsNum) {
		return key.hashCode() % partitionsNum;
	}

	/**
	 * put results into temp files
	 * 
	 * @param filePath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Map<Integer, String> partition(String filePath){
		
		Map<Integer, String> postShufflePathMap = new HashMap<Integer, String>();
		TmpResult reader;
		try {
			reader = new TmpResult(filePath, this.bufferSize);
			this.shuffleIDMap.clear();
			while (reader.hasNext()) {
				TmpKVPair pair = reader.next();
				int postShuffleID = this.getPostShuffleID(pair.getKey(),
						pair.getValue(), this.partitionNum);
				if (!this.shuffleIDMap.containsKey(postShuffleID)) {
					String postShufflePath = this.tmpDir + File.separator
							+ "partition_" + postShuffleID;
					partitionPaths.add(postShufflePath);
					ObjectOutputStream os;
					try {
						os = new ObjectOutputStream(
								new FileOutputStream(postShufflePath));
						this.shuffleIDMap.put(postShuffleID, os);
						postShufflePathMap.put(postShuffleID, postShufflePath);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				ObjectOutputStream out = this.shuffleIDMap.get(postShuffleID);
				out.writeObject(pair);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (ObjectOutputStream os : this.shuffleIDMap.values()) {
			try {
				
				os.flush();
				os.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return postShufflePathMap;
	}
}
