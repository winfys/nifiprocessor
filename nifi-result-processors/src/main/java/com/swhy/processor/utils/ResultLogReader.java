package com.swhy.processor.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 读取集中委托日志格式的类
 * @author winfy
 *
 */
public class ResultLogReader {
	private static final Logger LOGGER = Logger.getLogger(ResultLogReader.class.getName());

	/**
	 * The current read record number.
	 */
	private long currentRecordNumber;

	/**
	 * The input file.
	 */
	private File input;

	/**
	 * The current record byte buffer.
	 */
	private byte[] currentRecordBuf = new byte[217];
	private boolean hasRecord = false;
	private int currentCount = 0;
	private byte[] extLenBuf = new byte[5];

	/**
	 * BufferedReader to read input file.
	 */
	private BufferedInputStream reader;

	/**
	 * A second reader used to calculate the number of records in the input
	 * file.
	 */
	private BufferedInputStream recordCounterReader;

	/**
	 * Constructs a result log file record reader.
	 *
	 * @param input
	 *            the input file
	 * @throws FileNotFoundException
	 *             thrown if the file does not exist
	 */
	public ResultLogReader(final File input) throws FileNotFoundException {
		this.input = input;
	}

	public void close() {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.log(Level.SEVERE, "Unable to close file ");
			}
		}
	}

	public String getDataSourceName() {
		return input.getAbsolutePath();
	}

	public Long getTotalRecords() {
		long totalRecords = 0;
		try {
			recordCounterReader = new BufferedInputStream(new FileInputStream(input));
			currentCount = reader.read(currentRecordBuf, 0, 217);
			while (currentCount == 217) {
				totalRecords++;
				if (currentRecordBuf[0] == 'E') {
					if (recordCounterReader.read() == 'E' && recordCounterReader.read(extLenBuf) == 5) {
						StringBuilder builder = new StringBuilder();
						builder.append(extLenBuf);
						String extLen = builder.toString();
						Long extDataLen = Long.parseLong(extLen, 16);
						if (recordCounterReader.skip(extDataLen) != extDataLen) {
							LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
							break;
						}
					} else {
						LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
						break;
					}
				}
				currentCount = reader.read(currentRecordBuf, 0, 217);
			}
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.SEVERE, "Unable to calculate total records number", e);
			return null;
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Unable to calculate total records number", e);
			return null;
		} finally {
			if (recordCounterReader != null) {
				try {
					recordCounterReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return totalRecords;
	}

	public boolean hasNextRecord() {
		if (hasRecord) {
			return true;
		} else {
			try {
				currentCount = reader.read(currentRecordBuf, 0, 217);
				if (currentCount == 217) {
					hasRecord = true;
				} else if (currentCount == -1) {
					LOGGER.log(Level.INFO, "the end of the log file is reached");
					hasRecord = false;
				} else {
					hasRecord = false;
					LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
				}
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.log(Level.SEVERE, "Unable to read record", e);
			}
		}
		return hasRecord;
	}

	public void open() throws Exception {
		currentRecordNumber = 0;
		hasRecord = false;
		currentCount = 0;
		try {
			reader = new BufferedInputStream(new FileInputStream(input));
		} catch (FileNotFoundException e) {
			throw new Exception("Unable to find file " + input.getName(), e);
		}
	}

	public byte[] readNextRecord() throws Exception {
		try {
			if (!hasRecord) {
				try {
					currentCount = reader.read(currentRecordBuf, 0, 217);
					if (currentCount == 217) {
						hasRecord = true;
					} else if (currentCount == -1) {
						LOGGER.log(Level.INFO, "the end of the log file is reached");
						hasRecord = false;
					} else {
						hasRecord = false;
						LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
					}
				} catch (IOException e) {
					e.printStackTrace();
					LOGGER.log(Level.SEVERE, "Unable to read record", e);
				}
			}
			if (hasRecord) {
				if (currentRecordBuf[0] == 'E') {
					int c = reader.read();
					if (c == 'E') {
						int count = reader.read(extLenBuf, 0, 5);
						if (count == 5) {
							String extLen = new String(extLenBuf, "GB18030");
							int extDataLen = Integer.parseInt(extLen, 16);
							byte[] extDataBuf = new byte[extDataLen];
							int len = reader.read(extDataBuf, 0, extDataLen);
							if (len == extDataLen) {
								byte[] buffer = new byte[217 + 1 + 5 + extDataLen];
								System.arraycopy(currentRecordBuf, 0, buffer, 0, 217);

								buffer[217] = 'E';

								System.arraycopy(extLenBuf, 0, buffer, 218, 5);

								System.arraycopy(extDataBuf, 0, buffer, 223, extDataLen);
								hasRecord = false;
								currentCount = 0;

								return buffer;

							} else if (len == -1) {
								LOGGER.log(Level.INFO, "the end of the log file is reached");
							} else {
								LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
							}
						} else if (count == -1) {
							LOGGER.log(Level.INFO, "the end of the log file is reached");
						} else {
							LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
						}
					} else if (c == -1) {
						LOGGER.log(Level.INFO, "the end of the log file is reached");
					} else {
						LOGGER.log(Level.SEVERE, "Format of the log file is invalid");
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.log(Level.SEVERE, "Unable to read record", e);
			throw new Exception("Unable to read file " + input.getName(), e);
		} finally {
			if (hasRecord) {
				hasRecord = false;
				currentCount = 0;
				return currentRecordBuf;
			}
		}
		return null;
	}
}
