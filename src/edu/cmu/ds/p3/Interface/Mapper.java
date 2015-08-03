package edu.cmu.ds.p3.Interface;

import java.io.Serializable;

import edu.cmu.ds.p3.util.TmpKV2TmpResult;

/**
 * The Mapper interface where the user program has to extend from
 */
public interface Mapper extends Serializable {

	/**
	 * @param key
	 * @param value
	 * @param collector
	 * @throws Exception
	 */
	public abstract void Map(long key, String value, TmpKV2TmpResult collector)
			throws Exception;

	public int getMapperNum();

	public int getReducerNum();
}
