package edu.cmu.ds.p3.util;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * The iterator that iterates on each intermediate pair
 */
public class TmpResult implements Iterator<TmpKVPair>,
		Closeable {

	private int bufferSize = 1000;
	private ObjectInputStream is;
	private List<TmpKVPair> pairs = new LinkedList<TmpKVPair>();
	private ListIterator<TmpKVPair> pairItr = null;

	public int getBufferSize() {
		return bufferSize;
	}

	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}

	public TmpResult(String filePath, int bufferSize)
			throws FileNotFoundException, IOException {
		this.is = new ObjectInputStream(new FileInputStream(filePath));
		this.bufferSize = bufferSize;
	}

	/**
	 * load key-value pair into a list.
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private int load(List list, ObjectInputStream is, int size) {
		int num = 0;
		for (int i = 0; i < size; i++) {
			TmpKVPair pair = null;			
			try {
				pair = (TmpKVPair) is.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}

			if (pair == null)
				break;
			list.add(pair);
			num++;
		}
		return num;
	}

	@Override
	public boolean hasNext() {

		if (this.pairItr == null || !this.pairItr.hasNext()) {
			this.pairs.clear();
			int k = 0;
			k = this.load(this.pairs, this.is, this.bufferSize);
			this.pairItr = this.pairs.listIterator();
			if (k == 0)
				return false;
		}

		return this.pairItr.hasNext();
	}

	@Override
	public TmpKVPair next() {
		if (this.pairItr == null || !this.pairItr.hasNext())
			return null;
		return this.pairItr.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		if (this.is != null)
			this.is.close();
	}
}
