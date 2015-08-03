package edu.cmu.ds.p3.combiner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import edu.cmu.ds.p3.util.*;

/**
 * This serves as a tool to merge sort the intermediate records output from the
 * mapper
 */
public class Sorter {

	private int bufferSize;
	private String tmpDir;

	/**
	 * merge two files.
	 * 
	 * @param filePath1
	 * @param filePath2
	 * @param mergedFilePath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void mergeTmpFiles(String filePath1, String filePath2,
			String mergedFilePath) throws FileNotFoundException, IOException {
		//int pairNum = 0;
		TmpResult rst1 = new TmpResult(
				filePath1, this.bufferSize);
		TmpResult rst2 = new TmpResult(
				filePath2, this.bufferSize);
		ObjectOutputStream os = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream(mergedFilePath)));
		TmpKVPair pair1 = rst1.hasNext() ? rst1.next() : null;
		TmpKVPair pair2 = rst2.hasNext() ? rst2.next() : null;
		TmpKVPair curPair = null;
		TmpKVPair lastPair = null;
		while (pair1 != null || pair2 != null) {
			if (pair1 != null && (pair2 == null || pair1.compareTo(pair2) <= 0)) {
				curPair = pair1;
				pair1 = rst1.hasNext() ? rst1.next() : null;
			} else {
				curPair = pair2;
				pair2 = rst2.hasNext() ? rst2.next() : null;
			}
			os.writeObject(curPair);
			//pairNum++;
			if (lastPair == null) {
				lastPair = curPair;
			} else if (curPair.compareTo(lastPair) == 0) {
				continue;
			} else {
				lastPair = curPair;
			}

		}
		os.flush();
		os.close();
		rst1.close();
		rst2.close();
		//System.out.println("pairNum: " + pairNum);
		return;
	}

	private String getFilePath(int phaseNum, int tmpFileSeq) {
		return this.tmpDir + File.separator + "tmp_" + phaseNum + "_"
				+ tmpFileSeq;
	}

	/**
	 * Sort the split files
	 */
	public void sortSplits(List<String> splitPaths, String sortedPath)
			throws FileNotFoundException, IOException {
		List<String> toMergePaths = new ArrayList<String>(splitPaths);
		int maxSeq = splitPaths.size();
		int lastSeq = -1;
		int phaseNum = 1;
		if (maxSeq != 1) {
			while (maxSeq > 1) {
				List<String> mergedPaths = new ArrayList<String>();
				lastSeq = -1;
				if (maxSeq % 2 != 0) {
					lastSeq = maxSeq;
					maxSeq = maxSeq - 1;
				}
				for (int i = 1; i <= maxSeq / 2; i++) {
					mergeTmpFiles(toMergePaths.get(i - 1),
							toMergePaths.get(i + maxSeq / 2 - 1),
							getFilePath(phaseNum + 1, i));
					mergedPaths.add(getFilePath(phaseNum + 1, i));
				}
				maxSeq = maxSeq / 2;

				// remains one file to merge
				if (lastSeq != -1) {
					renameFile(toMergePaths.get(lastSeq - 1),
							getFilePath(phaseNum + 1, maxSeq + 1), true);
					mergedPaths.add(getFilePath(phaseNum + 1, maxSeq + 1));
					maxSeq++;
				}

				phaseNum++;
				toMergePaths = mergedPaths;
			}
		}
		renameFile(toMergePaths.get(maxSeq - 1), sortedPath, true);
		return;
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
	
	public static boolean createDir(String path) {
		File file = new File(path);
		if (!file.exists()) {
			if (file.mkdir()) {
				return true;
			} else {
				return false;
			}
		} else {
			if (!file.isDirectory()) {
				return false;
			}
		}
		return true;
	}
	
	public static void renameFile(String filePath, String filePathRenamed,
			boolean deleteIfExist) throws IOException {
		File file = new File(filePath);
		File fileRenamed = new File(filePathRenamed);
		if (fileRenamed.exists()) {
			if (!deleteIfExist)
				throw new IOException("Temporary file " + filePathRenamed
						+ " exists.");
			else {
				if (!fileRenamed.delete())
					throw new IOException(
							"Cannot delete existing temporary file "
									+ filePathRenamed + ".");
			}
		}

		boolean success = file.renameTo(fileRenamed);
		if (!success) {
			throw new IOException("Fail to rename temporary file " + filePath
					+ " to " + filePathRenamed);
		}
	}
	
	public static void deleteFile(String filePath) throws IOException {
		File file = new File(filePath);
		if (file.exists()) {
			if (!file.delete())
				throw new IOException("Cannot delete file " + filePath + ".");
		}
	}
}
