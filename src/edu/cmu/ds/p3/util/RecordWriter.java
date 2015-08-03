package edu.cmu.ds.p3.util;


import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * writer a record to a file
 */
public class RecordWriter implements Closeable {

	private RandomAccessFile file;
	private long offset;
	private int length;
	private String value;
	private byte[] bytes;

	public RecordWriter(String filePath, long offset, int length)
			throws IOException {
		this.file = new RandomAccessFile(filePath, "rw");
		this.file.seek(offset);
		this.setLength(length);
	}

	public void close() throws IOException {
		this.file.close();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * parsing data
	 */
	private byte[] toBytes(int len) {
		bytes = new byte[len];
		byte[] valueBuf = value.getBytes();
		bytes[0] = new Integer(valueBuf.length).byteValue();
		for (int i = 0; i < Math.min(bytes.length, value.length()); i++)
			bytes[i + 1] = valueBuf[i];
		return bytes;
	}

	/**
	 * write value to the file
	 * 
	 * @throws IOException
	 */
	public void Write() throws IOException {
		this.file.write(toBytes(length));
	}
}

