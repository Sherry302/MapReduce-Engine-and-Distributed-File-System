package edu.cmu.ds.p3.slave;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import edu.cmu.ds.p3.Interface.*;
import edu.cmu.ds.p3.configuration.Config;
import edu.cmu.ds.p3.master.Master;
import edu.cmu.ds.p3.util.*;
import edu.cmu.ds.p3.combiner.*;

/**
 * execute the map or reduce task
 */
public class Slave {
	private boolean isAlive;
	private String slaveID;
	private Config config;
	private String masterAddr;
	private int masterPort;
	private long mapTaskID;
	
	public String receivedFile;
	public boolean isInterTransReady;
	private List<String> partitionPaths;

	public Slave(Config config, String slaveID) throws UnknownHostException {
		this.setConfig(config);
		this.setSlaveID(slaveID);
		this.isAlive = true;
		this.masterAddr = config.getMasterAddr();
		this.masterPort = config.getMasterPort();
		this.receivedFile = slaveID + "_";
		this.partitionPaths = new ArrayList<String>();
	}

	public boolean isAlive() {
		return isAlive;
	}

	public void shutdown() throws IOException {
		isAlive = false;
	}

	/**
	 * execute map task
	 * 
	 * @throws IOException
	 */
	public void map(Message msg) {
		MapTask mapTask = (MapTask) msg.getTask();
		mapTaskID = mapTask.getTaskID();
		Mapper mapper = mapTask.getMTask();
		InputSplit inputsplit = mapTask.getSplit();
	
		RecordReader reader = new RecordReader(inputsplit);
		Sorter sorter = new Sorter();
		String tmpDir = "tmp" + File.separator + mapTask.getSlaveID()
				+ File.separator + mapTask.getTaskID() + File.separator + "map";
		String sortPath = tmpDir + File.separator + "intermidateResult_sortedByMapper";
		new File(tmpDir).mkdirs();
		try {
			Record record = reader.getRecord();
			TmpKV2TmpResult trans = new TmpKV2TmpResult();
			trans.setBufferSize(config.getBufferSize());
			trans.setTmpDir(tmpDir);

			// run map task
			mapper.Map(record.getKey(), record.getValue(), trans);
			while ((record = reader.nextRecord()) != null) {
				mapper.Map(record.getKey(), record.getValue(), trans);
			}
			trans.emit();
			sorter.setBufferSize(config.getBufferSize());
			sorter.setTmpDir(tmpDir);
			sorter.sortSplits(trans.getSplitPaths(), sortPath);

			// partition
			Shuffler partition = new Shuffler();
			partition.setBufferSize(config.getBufferSize());
			partition.setPartitionNum(mapTask.getMTask().getReducerNum());
			partition.setTmpDir(tmpDir);
			Map<Integer, String> res = partition.partition(sortPath);
			
			partitionPaths = partition.getPartitionPaths();
			isInterTransReady = true;

			// send back msg
			mapTask.setTmpResults(res);
			Message message = new Message(Message.TYPE.MAPPER);
			message.setTask(mapTask);
			Message.sendRequest(masterAddr, masterPort, 5000, message);
		} catch (Exception e) {
			e.printStackTrace();
			sendException(msg);
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
				}
		}
		System.out.println("mapping done. The intermidate result is in: " + sortPath);
		System.out.print("$ ");
	}

	/**
	 * execute the reduce task
	 * 
	 * @param msg
	 * @throws InterruptedException 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void reduce(Message msg) throws InterruptedException {
		ReduceTask reduceTask = (ReduceTask) msg.getTask();
		Reducer reducer = reduceTask.getRTask();
		Sorter sorter = new Sorter();

		// prepare the paths and parameters
		int bufferSize = config.getBufferSize();
		List<String> inputs = reduceTask.getInputFiles();
		String tmpReduceDir = "tmp" + File.separator + slaveID
				+ File.separator + reduceTask.getTaskID() + File.separator
				+ "reduce";
		String id = "output_" + reduceTask.getTaskID() + "_" + slaveID;
		String outputFile = reduceTask.getOutputFolder() + File.separator
				+ id;
		String sortPath = tmpReduceDir + File.separator + "result_sortedByReducer";
		new File(tmpReduceDir).mkdirs();
		new File(reduceTask.getOutputFolder()).mkdirs();
		
		// get mappers' workID
		String workers_id[] = new String[inputs.size()]; 
		for (int i = 0; i < inputs.size(); i++){
			String[] subDirs = inputs.get(i).split(Pattern.quote(File.separator));
			workers_id[i] = subDirs[1];			
		}
				
		List<Thread> runningThreads = new ArrayList<Thread>();
		for (int j = 0; j < workers_id.length; j++) {
			final String worker_id = workers_id[j];
			final String path =  worker_id + "_";

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						int iport = config.getClientInterResTranPort(worker_id);
						Socket sock = new Socket(config.getSlaveAddr(worker_id), iport);
					
						RandomAccessFile ifile = new RandomAccessFile(path, "rw");										
						ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
						String sfile = (String) in.readObject();
						ifile = new RandomAccessFile(path, "rw");
						ifile.write(sfile.getBytes());
						in.close();
						sock.close();
					} catch (Exception e) {
						e.printStackTrace();
						Master.exceptionHandler(worker_id);
					}
				}
			});
			runningThreads.add(t);
			t.start();
		}
		
		for (Thread t : runningThreads)
			t.join();
		runningThreads.clear();


		// do the reduce
		try {
			RecordWriter writer = new RecordWriter(outputFile, 0,
					reducer.getRecordLength());
			sorter.setBufferSize(bufferSize);
			sorter.setTmpDir(tmpReduceDir);
			sorter.sortSplits(inputs, sortPath);
			reduce(sortPath, bufferSize, reducer, writer);
			Message message = new Message(Message.TYPE.REDUCER);
			message.setTask(reduceTask);
			Message.sendRequest(masterAddr, masterPort, 5000, message);
			System.out.println("reducing done. Check the result in output folder.");
			System.out.print("$ ");
		} catch (Exception e) {
			e.printStackTrace();
			sendException(msg);
		}
	}

	/**
	 * Notify the master for any exceptions
	 */
	private void sendException(Message msg) {
		try {
			Message exceptionMsg = new Message(Message.TYPE.EXCEPTION);
			exceptionMsg.setTask(msg.getTask());
			Message.sendRequest(masterAddr, masterPort, 5000, exceptionMsg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * combine values and execute the reduce method
	 * 
	 * @throws Exception
	 */
	private void reduce(String path, int bufferSize, Reducer reducer,
			RecordWriter writer) throws Exception {
		TmpResult reader = new TmpResult(
				path, bufferSize);
		TmpKVPair curPair = null;
		TmpKVPair lastPair = null;
		String key = null;
		List<String> values = new ArrayList<String>();
		while (reader.hasNext()) {
			curPair = reader.next();
			if (lastPair == null) {
				lastPair = curPair;
				key = lastPair.getKey();
			}
			if (lastPair.getKey().compareTo(curPair.getKey()) != 0) {
				reducer.Reduce(key, values.iterator(), writer);
				lastPair = curPair;
				key = lastPair.getKey();
				values.clear();
			}
			values.add(curPair.getValue());
		}
		reader.close();
		writer.close();
	}

	public Config getConfig() {
		return config;
	}

	public void setConfig(Config config) {
		this.config = config;
	}

	public String getWorkerID() {
		return slaveID;
	}

	public void setSlaveID(String workerID) {
		this.slaveID = workerID;
	}

	public long getMapTaskID() {
		return this.mapTaskID;
	}
	
	public List<String> getPartitionPaths() {
		return partitionPaths;
	}
}
