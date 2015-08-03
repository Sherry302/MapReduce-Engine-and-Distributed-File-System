package edu.cmu.ds.p3.Interface;

import java.io.Serializable;
import java.util.Iterator;

import edu.cmu.ds.p3.util.RecordWriter;

/**
 * The Reducer interface where the user program has to extend from
 */
public interface Reducer extends Serializable {

	/**
	 * @throws Exception
	 */
	public abstract void Reduce(String key, Iterator<String> values,
			RecordWriter writer) throws Exception;

	public int getRecordLength();
}
