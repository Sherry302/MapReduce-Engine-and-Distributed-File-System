package edu.cmu.ds.p3.master;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.cmu.ds.p3.Interface.Mapper;
import edu.cmu.ds.p3.Interface.Reducer;
import edu.cmu.ds.p3.configuration.Config;
import edu.cmu.ds.p3.util.InputSplit;
import edu.cmu.ds.p3.util.MapTask;
import edu.cmu.ds.p3.util.Message;
import edu.cmu.ds.p3.util.ReduceTask;
import edu.cmu.ds.p3.util.Task;

/**
 * coordinates and monitors tasks
 */
public class Master {

	private static Config config;
	private int maxMaps;
	private int maxReduces;
	private boolean isAlive;
	private static volatile Map<String, List<Task>> slaves;
	private static volatile Map<Long, List<Map<Integer, String>>> taskResultCollector;
	private HealthyChecker checker;

	public Master(Config config) {
		Master.config = config;
		slaves = new ConcurrentHashMap<String, List<Task>>();
		taskResultCollector = new ConcurrentHashMap<Long, List<Map<Integer, String>>>();
		maxMaps = config.getMaxMaps();
		maxReduces = config.getMaxReduces();
		isAlive = false;
		checker = new HealthyChecker(slaves, config);
	}

	/**
	 * Process the exception
	 */
	public void exception(Message message) throws SocketTimeoutException,
			UnknownHostException, IOException, ClassNotFoundException {

		/* remove the tasks from the job tracking table */
		for (String s : slaves.keySet()) {
			int i = 0;
			while (i < slaves.get(s).size()) {
				if (slaves.get(s).get(i).getTaskID() == message.getTask()
						.getTaskID()) {
					slaves.get(s).remove(i);
				} else {
					i++;
				}
			}
		}

		/* print out or send back the exception message to the task issuer */
		if (message.getTask().getHandler().equals("Master")) {
			System.out.println("Task " + message.getTask().getTaskID()
					+ " can't be finished.");
		} else {
			Message msg = new Message(Message.TYPE.RESULT);
			msg.setMsg("Task " + message.getTask().getTaskID()
					+ " can't be finished.");
			String handler = message.getTask().getHandler();
			Message.sendRequest(config.getSlaveAddr(handler),
					config.getSlavePort(handler), 5000, msg);
		}
	}

	/**
	 * do works when reduce task is going to finish
	 */
	public void reducer(Message message) {

		String slaveID = message.getTask().getSlaveID();
		boolean lastOne = removeTask(slaveID, message.getTask().getTaskID());

		// wait for all reduce task to finish
		if (lastOne) {
			try {
				String handler = message.getTask().getHandler();
				if (handler.equals("Master")) {
					System.out.println("Task from master finished. Your finished submitted job: ");
				}
				else {
					Message msg = new Message(Message.TYPE.RESULT);
					msg.setMsg("Task " + message.getTask().getTaskID()
							+ " is finished.");
					Message.sendRequest(config.getSlaveAddr(handler),
							config.getSlavePort(handler), 5000, msg);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * when the map task is finishing, prepare for reducing
	 */
	public synchronized void mapper(Message message)
			throws InterruptedException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, Exception {
		boolean lastOne = removeTask(message.getTask().getSlaveID(), message
				.getTask().getTaskID());

		// failure happened
		if (!taskResultCollector.containsKey(message.getTask().getTaskID()))
			return;
		// collect the intermediate result
		List<Map<Integer, String>> taskResults = taskResultCollector
				.get(message.getTask().getTaskID());
		MapTask task = (MapTask) message.getTask();
		taskResults.add(task.getTmpResults());
		if (lastOne) {
			List<Thread> runningThreads = new ArrayList<Thread>();
			List<String> freeWorkers = getFreeSlaves(task
					.getTmpResults().size());
			long taskID = System.currentTimeMillis();
			Reducer reduce;
			try {
				@SuppressWarnings("unchecked")
				Class<Reducer> reduceClass = (Class<Reducer>) (Class
						.forName("edu.cmu.ds.p3.example."
								+ task.getReducerClass()));
				Constructor<?> reduceConstructor = reduceClass.getConstructor();
				reduce = (Reducer) reduceConstructor.newInstance();
			} catch (ClassNotFoundException e1) {
				message.setMsg("Reduce Class cannot be found.");
				return;
			}
			for (int i = 0; i < freeWorkers.size(); i++) {

				// collect info for a reduce task
				final String slaveID = freeWorkers.get(i);
				final ReduceTask reduceTask = new ReduceTask();
				reduceTask.setRTask(reduce);
				reduceTask.setTaskID(taskID);
				reduceTask.setReducerClass(task.getReducerClass());
				reduceTask.setOutputFolder(task.getOutputFolder());
				reduceTask.setReducerID(i);
				reduceTask.setHandler(task.getHandler());
				reduceTask.setSlaveID(slaveID);
				reduceTask.setInputFiles(new ArrayList<String>());

				for (int j = 0; j < taskResults.size(); j++)
					reduceTask.getInputFiles().add(taskResults.get(j).get(i));

				/* Send out the reduce tasks */
				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							Message msg = new Message(Message.TYPE.REDUCER);
							msg.setTask(reduceTask);
							Message.sendRequest(config.getSlaveAddr(slaveID),
									config.getSlavePort(slaveID), 0, msg);
						} catch (Exception e) {
							Master.exceptionHandler(slaveID);
						}
					}

				});
				runningThreads.add(t);
				slaves.get(freeWorkers.get(i)).add(reduceTask);
				t.start();
			}

			// send out all reduce tasks
			taskResultCollector.remove(task.getTaskID());
			try {
				for (Thread t : runningThreads)
					t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			runningThreads.clear();
		}
	}

	/**
	 * send map tasks to workers
	 */
	public void submit(Message message, Socket clientSock) {
		String[] args = message.getMsg().split(" ");
		String mapperClass = args[0];
		String inputFile = args[1];
		String reducerClass = args[2];
		String outputFolder = args[3];
		try {
			Mapper map;
			@SuppressWarnings("unchecked")
			Class<Mapper> mapClass = (Class<Mapper>) (Class
					.forName("edu.cmu.ds.p3.example." + mapperClass));
			Constructor<?> mapConstructor = mapClass.getConstructor();
			map = (Mapper) mapConstructor.newInstance();
			if (map.getMapperNum() > Math.min(maxMaps, slaves.size())
					|| map.getReducerNum() > Math.min(maxReduces,
							slaves.size())) {
				message.setMsg("map-reduce maximum size limit");
				return;
			}

			// partition input file
			InputSplit splitter = new InputSplit(inputFile, map.getMapperNum());
			ArrayList<InputSplit> splits = splitter.getSplits();
			ArrayList<String> splitStrings = splitter.getSplitedStrings();

			// collect all info for a map task
			List<Thread> runningThreads = new ArrayList<Thread>();
			List<String> freeSlaves = getFreeSlaves(map.getMapperNum());
			long taskID = System.currentTimeMillis();
			taskResultCollector.put(taskID,
					new ArrayList<Map<Integer, String>>());
			for (int i = 0; i < freeSlaves.size(); i++) {

				/* Prepare the payload and sent out the map tasks */
				final String slaveID = freeSlaves.get(i);
				final MapTask mapTask = new MapTask();
				final String sfile = splitStrings.get(i);
				
				mapTask.setTaskID(taskID);
				mapTask.setMapperClass(mapperClass);
				mapTask.setInputFile(inputFile);
				mapTask.setOutputFolder(outputFolder);
				mapTask.setReducerClass(reducerClass);
				mapTask.setMapperID(i);
				mapTask.setHandler(message.getId());
				mapTask.setSlaveID(slaveID);
				mapTask.setSplit(splits.get(i));
				mapTask.setMTask(map);

				Thread t = new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							
							int cport = config.getClientInputFileTranPort(slaveID);
							Socket sock = new Socket(config.getSlaveAddr(slaveID), cport);
							ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
							out.writeObject(sfile);
							out.flush();
							out.close();
							sock.close();
							
							Message msg = new Message(Message.TYPE.MAPPER);
							msg.setTask(mapTask);
							Message.sendRequest(config.getSlaveAddr(slaveID),
									config.getSlavePort(slaveID), 0, msg);
						} catch (Exception e) {
							Master.exceptionHandler(slaveID);
						}
					}

				});
				runningThreads.add(t);
				slaves.get(freeSlaves.get(i)).add(mapTask);
				t.start();
			}
			for (Thread t : runningThreads)
				t.join();
			runningThreads.clear();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Handle any exceptions, restart tasks
	 */
	public static void exceptionHandler(String slaveID) {

		// failure happens
		if (!slaves.containsKey(slaveID))
			return;
		List<Task> remainingTasks = slaves.get(slaveID);
		slaves.remove(slaveID);
		String substitution = null;
		try {

			// look for replacement
			for (Task p : remainingTasks) {
				List<String> allSlaves = new ArrayList<String>(
						slaves.keySet());
				Collections.sort(allSlaves, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						return ((Integer) slaves.get(o1).size())
								.compareTo((Integer) slaves.get(o2).size());
					}
				});
				for (String s : allSlaves) {
					if (!containsTask(p.getTaskID(), slaves.get(s))) {
						substitution = s;
						break;
					}
				}

				// notice the user
				if (substitution == null) {
					System.out.println("fail to handle exception");
					taskResultCollector.remove(p.getTaskID());
					if (p.getHandler().equals("Master")) {
						System.out.println("Task " + p.getTaskID()
								+ " is failed on " + p.getSlaveID());
					} else {
						Message msg = new Message(Message.TYPE.RESULT);
						msg.setMsg("Task " + p.getTaskID() + " is failed on "
								+ p.getSlaveID());
						Message.sendRequest(
								config.getSlaveAddr(p.getHandler()),
								config.getSlavePort(p.getHandler()), 5000, msg);

					}
					return;
				}

				// if replacement found
				slaves.get(substitution).add(p);
				Message msg = null;
				if (p instanceof MapTask) {
					((MapTask) p).setSlaveID(substitution);
					msg = new Message(Message.TYPE.MAPPER);
					msg.setTask(p);
				} else if (p instanceof ReduceTask) {
					((ReduceTask) p).setSlaveID(substitution);
					msg = new Message(Message.TYPE.REDUCER);
					msg.setTask(p);
				}
				Message.sendRequest(config.getSlaveAddr(substitution),
						config.getSlavePort(substitution), 5000, msg);
			}
		} catch (Exception e) {
			exceptionHandler(substitution);
		}
	}

	/**
	 * Remove a task
	 */
	private boolean removeTask(String workerID, long taskID) {
		boolean lastOne = true;
		for (String s : slaves.keySet()) {
			if (s.equals(workerID)) {
				int i = 0;
				while (i < slaves.get(s).size()) {
					if (slaves.get(s).get(i).getTaskID() == taskID)
						slaves.get(s).remove(i);
					else
						i++;
				}
			} else {
				for (int i = 0; i < slaves.get(s).size(); i++)
					lastOne = lastOne
							&& !(slaves.get(s).get(i).getTaskID() == taskID);
			}
		}
		return lastOne;
	}

	/**
	 * get workers with least burden
	 */
	public List<String> getFreeSlaves(int n) {
		List<String> allSlaves = new ArrayList<String>(slaves.keySet());
		Collections.sort(allSlaves, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return ((Integer) slaves.get(o1).size())
						.compareTo((Integer) slaves.get(o2).size());
			}
		});
		return allSlaves.subList(0, n);
	}

	/**
	 * check if contains the task
	 */
	public static boolean containsTask(long taskID, List<Task> pool) {
		for (Task t : pool) {
			if (t.getTaskID() == taskID)
				return true;
		}
		return false;
	}

	public void shutdown() throws IOException {
		isAlive = false;
	}

	public boolean isAlive() {
		return isAlive;
	}

	/**
	 * add a new worker
	 */
	public void register(String slave) {
		slaves.put(slave, new ArrayList<Task>());
	}

	/**
	 * start the facility
	 */
	public void start() {
		this.isAlive = true;
		new Thread(this.checker).start();
	}

	/**
	 * list all tasks
	 */
	public void monitor(Message msg) {
		if (slaves.containsKey(msg.getMsg())) {
			msg.setMsg(slaves.get(msg.getMsg()).size() + "");
		} else {
			msg.setMsg(0 + "");
		}
	}
}
