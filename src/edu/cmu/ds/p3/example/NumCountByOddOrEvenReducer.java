package edu.cmu.ds.p3.example;

import java.io.IOException;
import java.util.Iterator;

import edu.cmu.ds.p3.Interface.Reducer;
import edu.cmu.ds.p3.util.RecordWriter;

/**
 * A reducer example to count the total number of even / odd
 * 
 */
public class NumCountByOddOrEvenReducer implements Reducer {

	private static final long serialVersionUID = 1L;
	
	public NumCountByOddOrEvenReducer() {

	}

	@Override
	public int getRecordLength() {
		return 128;
	}

	@Override
	public void Reduce(String key, Iterator<String> values, RecordWriter writer)
			throws Exception {
		long total = 0;
		while (values.hasNext()) {
			values.next();
			total++;
		}
		writer.setValue(key + " total " + total + "\n");
		try {
			writer.Write();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
