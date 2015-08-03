package edu.cmu.ds.p3.util;


import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Closeable;

public class RecordReader implements Closeable {
	/**
	 * Make the recoder according to the size of request and make record
	 */
	private long key;
	private byte[] bytes;
	private String value = null;
	private InputSplit splitBlock;
	private int length;
	private RandomAccessFile file;
	private long position;
	private long end;

	public class ByteArrayBuffer {
		private byte[] buffer;
		private int len;
		public ByteArrayBuffer(int size) {
			super();
			if (size < 0) {
				throw new IllegalArgumentException(
						"Buffer capacity may not be negative");
			}
			this.buffer = new byte[size];
		}

		private void expand(int newlen) {
			byte newbuffer[] = new byte[Math.max(this.buffer.length << 1,
					newlen)];
			System.arraycopy(this.buffer, 0, newbuffer, 0, this.len);

			this.buffer = newbuffer;

		}

		public void append(final byte[] b, int offset, int len) {
			if (b == null) {
				return;
			}

			if ((offset < 0) || (offset > b.length) || (len < 0) ||
			((offset + len) < 0) || ((offset + len) > b.length)) {
				throw new IndexOutOfBoundsException();
			}

			if (len == 0) {
				return;
			}

			int newlen = this.len + len;

			if (newlen > this.buffer.length) {
				this.expand(newlen);
			}
			System.arraycopy(b, offset, this.buffer, this.len, len);
			this.len = newlen;

		}

		public void append(int b) {
			int newlen = this.len + 1;
			if (newlen > this.buffer.length) {
				expand(newlen);
			}
			this.buffer[this.len] = (byte) b;
			this.len = newlen;
		}

		public void append(final char[] b, int offset, int len) {
			if (b == null) {
				return;
			}

			if ((offset < 0) || (offset > b.length) || (len < 0) ||
			((offset + len) < 0) || ((offset + len) > b.length)) {
				throw new IndexOutOfBoundsException();
			}

			if (len == 0) {
				return;
			}

			int oldlen = this.len;

			int newlen = oldlen + len;
			if (newlen > this.buffer.length) {
				expand(newlen);
			}

			for (int i1 = offset, i2 = oldlen; i2 < newlen; i1++, i2++) {
				this.buffer[i2] = (byte) b[i1];
			}
			this.len = newlen;

		}

		public void clear() {
			this.len = 0;
		}

		public byte[] toByteArray() {

			byte[] b = new byte[this.len];

			if (this.len > 0) {

				System.arraycopy(this.buffer, 0, b, 0, this.len);

			}

			return b;

		}

		public int byteAt(int i) {

			return this.buffer[i];

		}

		public int size() {

			return this.buffer.length;

		}

		public int length() {

			return this.len;

		}

		public byte[] buffer() {

			return this.buffer;

		}

		public void setLength(int len) {

			if (len < 0 || len > this.buffer.length) {

				throw new IndexOutOfBoundsException();

			}

			this.len = len;

		}

		public boolean isEmpty() {

			return this.len == 0;

		}

		public boolean isFull() {

			return this.len == this.buffer.length;

		}

	}

	/**
	 * make a record for small size file
	 */
	public void makeRecord() throws IOException {

		long start = splitBlock.getStart();
		long end = start + splitBlock.getLength();
		final String path = splitBlock.getPath();
		file = new RandomAccessFile(path, "r");
		boolean skipFirstLine = false;

		if (start != 0) {
			skipFirstLine = true;
			--start;
			file.seek(start);
		}

		if (skipFirstLine) {
			String none = file.readLine();
			start = start + none.length();
			file.seek(start);
		}
		// Position is the actual start for each line
		this.key = start;
		if (start <= end) {
			this.bytes = readlines(file, (int) start, (int) (end));
		}
		this.value = new String(this.bytes);
	}

	/**
	 * Accoring to the byte-baesd split to form record
	 */
	public byte[] readlines(RandomAccessFile file, int position, int end)
			throws IOException {
		int byteSum = 0;
		ByteArrayBuffer mReadBuffer = new ByteArrayBuffer(0);

		while (position < end) {
			String s = file.readLine();
			int offset = 0;
			if (s != null) {
				byte[] buf = s.getBytes();
				mReadBuffer.append(buf, 0, buf.length);
				offset = buf.length;
			} else {
				offset = 500;
			}
			position = position + offset;
			byteSum = byteSum + offset;
		}
		this.length = byteSum;
		return mReadBuffer.buffer();

	}

	public RecordReader(InputSplit splitblock) {
		this.splitBlock = splitblock;
	}

	public long getkey() {

		return key;
	}

	public byte[] getbytes() {

		return bytes;
	}

	public String getvalue() {

		return value;
	}

	/**
	 * Get the record line by line
	 */
	public Record getRecord() throws IOException {
		long start = splitBlock.getStart();
		this.end = start + splitBlock.getLength();

		final String path = splitBlock.getPath();

		file = new RandomAccessFile(path, "r");

		boolean skipFirstLine = false;

		if (start != 0) {
			skipFirstLine = true;
			--start;
			file.seek(start);
		}

		if (skipFirstLine) {
			String none = file.readLine();
			start = start + none.length();
			file.seek(start);

		}
		this.position = start;
		// Position is the actual start
		this.key = start;

		if (start <= end) {
			this.bytes = readline(file, (int) position);
		}
		if (this.bytes != null) {
			this.value = new String(this.bytes);
		}

		Record record = new Record();
		record.setKey(position);
		record.setValue(value);

		return record;

	}

	public Record nextRecord() throws IOException {

		Record record = new Record();
		record.setKey(position);
		record.setValue(value);

		file.seek(this.position);
		if (this.position < this.end) {
			this.bytes = readline(this.file, (int) this.position);
			this.value = new String(this.bytes);

			record.setValue(value);

			return record;
		} else {
			return null;
		}
	}

	/**
	 * read a line for mapper to iteration
	 */
	public byte[] readline(RandomAccessFile file, int position)
			throws IOException {
		ByteArrayBuffer mReadBuffer = new ByteArrayBuffer(0);

		file.seek(this.position);
		String s = file.readLine();
		int offset = 0;
		if (s != null) {
			byte[] buf = s.getBytes();

			mReadBuffer.append(buf, 0, buf.length);
			offset = buf.length;

			if (offset == 0) {
				this.position = this.position + 1;
				return readline(file, (int) (this.position));
			}

		} else {
			offset = 500;
		}

		this.position = this.position + offset;
		return mReadBuffer.buffer();

	}

	public int getLength() {
		return length;
	}

	public void close() throws IOException {
		this.file.close();
	}

}
