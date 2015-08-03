package edu.cmu.ds.p3.example;

import edu.cmu.ds.p3.Interface.Mapper;
import edu.cmu.ds.p3.util.TmpKV2TmpResult;

/**
 * A mapper example to output even numbers and odd numbers
 */
public class NumCountByOddOrEvenMapper implements Mapper {

	private static final long serialVersionUID = 1L;

	public NumCountByOddOrEvenMapper() {

	}

	@Override
	public int getMapperNum() {
		return 2;
	}

	@Override
	public int getReducerNum() {
		return 1;
	}

	@Override
	public void Map(long key, String value, TmpKV2TmpResult collector) throws Exception {
		String[] words = value.split(" ");

		for (String s : words) {
			if (s.equals("") || s.equals(" ")){	
				continue;
			}
				
			try {
				if (s.equals("s"))
				{
					collector.collect("s", "1");
				}
				else if (Integer.parseInt(s) % 2 == 0) {
					collector.collect("Even", "1");
				}
				else{
					collector.collect("Odd", "1");
				}
			} catch (NumberFormatException e) {
                 e.printStackTrace();
			}
		}
	}
}
