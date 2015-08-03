package edu.cmu.ds.p3.example;

import edu.cmu.ds.p3.Interface.Mapper;
import edu.cmu.ds.p3.util.TmpKV2TmpResult;

/**
 * count words, key: word value: 1
 */
public class WordCountMapper implements Mapper {

	private static final long serialVersionUID = 1L;

	public WordCountMapper() {

	}

	@Override
	public void Map(long key, String value, TmpKV2TmpResult collector)
			throws Exception {
		String line = value.trim().replaceAll("[^a-zA-Z]+"," ").toLowerCase();
		if (line.length() == 0)
			return;
		String[] words = line.split(" ");
		for (String s : words) {
			if(s.equals("") || s.equals(" ")) 
				continue;
			collector.collect(s, "1");
		}
	}

	@Override
	public int getMapperNum() {
		return 2;
	}

	@Override
	public int getReducerNum() {
		return 1;
	}
}
