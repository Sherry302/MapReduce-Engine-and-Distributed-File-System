package edu.cmu.ds.p3.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.cmu.ds.p3.configuration.Config;
import edu.cmu.ds.p3.util.Message;

/**
 * serves as the entry and terminal of the worker process
 */
public class SlaveManager {
	private static Slave worker;
	private static Config config;
	private static BufferedReader reader;
	private static boolean running = true;
	private static SlaveServer server;
	private static SlaveInputFileTransListener fserver;
	private static SlaveInterResTransListener iserver;

	/**
	 * stop the worker
	 */
	public static void stopProgram() {
		running = false;
		try {
			reader.close();
		} catch (Exception e) {
		}
		server.stopWorker();
		System.exit(0);
	}

	/**
	 * ask to join the facility
	 */
	private static void register(String workerID) {
		Message msg = new Message(Message.TYPE.REGISTER);
		msg.setMsg(workerID);
		try {
			Message.sendRequest(config.getMasterAddr(), config.getMasterPort(),
					5000, msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Submit Command
	 */
	private static void dealWithSubmitCmd(String[] cmdLine, String input,
			String arg) {
		if (cmdLine.length != 5) {
			System.out
					.println("Usage: submit <MapClass> <InputFile> <ReduceClass> <OutputFolder>");
			return;
		}
		if (!new File(cmdLine[2]).exists()) {
			System.out.println(cmdLine[2] + " doesn't exist!");
			return;
		}

		// send out the message
		final Message msg = new Message(Message.TYPE.SUBMIT);
		msg.setMsg(input.substring(input.indexOf(" ") + 1));
		msg.setId(arg);
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Message responseMsg = Message.sendRequest(
							config.getMasterAddr(), config.getMasterPort(), 0,
							msg);
					if (responseMsg.getType() == Message.TYPE.SUBMIT) {
						System.out.println(responseMsg.getMsg());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	private static void dealWithStartCmd() {
		Message msg = new Message(Message.TYPE.START);
		msg.setMsg("START");
		try {
			Message responseMsg = Message.sendRequest(config.getMasterAddr(),
					config.getMasterPort(), 5000, msg);
			System.out.println(responseMsg.getMsg());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void dealWithStopCmd(String arg) {
		Message msg = new Message(Message.TYPE.STOP);
		msg.setMsg("STOP");
		try {
			Message responseMsg = Message.sendRequest(config.getMasterAddr(),
					config.getMasterPort(), 5000, msg);
			System.out.println(responseMsg.getMsg());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void dealWithMonitorCmd(String arg) {
		Message msg = new Message(Message.TYPE.MONITOR);
		msg.setMsg(arg);
		try {
		Message responseMsg = Message.sendRequest(config.getMasterAddr(),
				config.getMasterPort(), 5000, msg);
		System.out.println(responseMsg.getMsg());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws FileNotFoundException, IOException {
		if (args.length != 2) {
			System.out.println("Usage: Worker <Worker ID> <Properties File>");
			System.exit(0);
		}
		config = new Config(args[1]);
		worker = new Slave(config, args[0]);
		server = new SlaveServer(worker, config, args[0]);
		server.start();
		fserver = new SlaveInputFileTransListener(worker, config, args[0]);
		fserver.start();
		iserver = new SlaveInterResTransListener(worker, config, args[0]);
		iserver.start();
		register(args[0]);
		reader = new BufferedReader(new InputStreamReader(System.in));
		while (running) {
			try {
				System.out.print("$ ");
				String input = reader.readLine();
				if (input == null)
					continue;
				String[] cmdLine = input.split(" ");
				switch (cmdLine[0]) {
				case "submit":
					dealWithSubmitCmd(cmdLine, input, args[0]);
					break;
				case "start":
					dealWithStartCmd();
					break;
				case "stop":
					dealWithStopCmd(args[0]);
					break;
				case "monitor":
					dealWithMonitorCmd(args[0]);
					break;
				case "quit":
					stopProgram();
					break;
				default:
					System.out.println("Your command is not supported!");
                    break;
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
