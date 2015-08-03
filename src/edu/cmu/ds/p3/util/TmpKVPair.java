package edu.cmu.ds.p3.util;

import java.io.Serializable;

/**
 * output from map task
 */
public class TmpKVPair implements Serializable, Comparable<TmpKVPair> {

	private static final long serialVersionUID = 1L;
	private String mapKey;
	private String mapValue;

	public TmpKVPair(String key, String value) {
		mapKey = key;
		mapValue = value;
	}

	public String getKey() {
		return mapKey;
	}

	public String getValue() {
		return mapValue;
	}

	public void setKey(String key) {
		mapKey = key;
	}

	public void setValue(String value) {
		mapValue = value;
	}

	@Override
	public String toString() {
		return String.format("<%s: key=%s, value=%s>",
				TmpKVPair.class.getSimpleName(), mapKey, mapValue);
	}

	@Override
	public int compareTo(TmpKVPair o) {
		return this.mapKey.compareTo(o.mapKey);
	}

}
