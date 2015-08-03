package edu.cmu.ds.p3.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import edu.cmu.ds.p3.configuration.Config;
import edu.cmu.ds.p3.util.Message;
import edu.cmu.ds.p3.util.Task;

/**
 * The tool frequently check the running status of the client every 5s.
 */
public class HealthyChecker implements Runnable {
	private volatile boolean isTerminate;
	private  volatile ArrayList<Thread> runningThreads;
	private boolean isAlive;
	private Map<String, List<Task>> slaves;
	private Config config;

	public HealthyChecker(Map<String, List<Task>> slaves, Config config) {
		runningThreads = new ArrayList<Thread>();
		this.slaves = slaves;
		this.config = config;
	}

	@Override
	public void run() {
		isAlive = true;

		while (!isTerminate) {
			try {
				for (final String id : slaves.keySet()) {
					Thread t = new Thread(new Runnable() {

						@Override
						public void run() {

							try {
								// check the status of the worker
								Message msg = new Message(Message.TYPE.STATUS);
								msg.setMsg("QUERY");
								Message response = Message.sendRequest(
										config.getSlaveAddr(id),
										config.getSlavePort(id), 5000, msg);
								if (response.getType() == Message.TYPE.STATUS) {
									if (response.getMsg().equals("HEALTHY")) {
										return;
									}
								}
							} catch (Exception e) {
								// no reply
								Master.exceptionHandler(id);
							}

						}

					});
					runningThreads.add(t);
					t.start();
				}

				for (Thread t : runningThreads)
					t.join();
				runningThreads.clear();

				/* issue the command every 5 sec. */
				Thread.sleep(5000);

			} catch (ConcurrentModificationException e) {
				//e.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}catch (Exception e2) {
				
			}
		}
		isTerminate = false;
	}

	public void shutdown() throws IOException {
		if (!isAlive) {
			return;
		}
		isTerminate = true;
		while (isTerminate)
			;
		isAlive = false;
		System.out.println("HealthyChecker quiting...");
	}

	public boolean isAlive() {
		return isAlive;
	}

}
