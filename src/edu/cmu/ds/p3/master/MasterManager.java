package edu.cmu.ds.p3.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.cmu.ds.p3.configuration.Config;
import edu.cmu.ds.p3.util.Message;

/**
 * serves as the entry and terminal of the master process
 */
public class MasterManager {
	private static Master master;
	private static Config config;

	private static void dealWithSubmitCmd(String[] cmdLine, String input) {
		if (cmdLine.length != 5) {
			System.out.println("Usage: submit <MapClass> <InputFile> <ReduceClass> <OutputFolder>");
			return;
		}
		if (!new File(cmdLine[2]).exists()) {
			System.out.println(cmdLine[2] + " doesn't exist!");
			System.out.print("$ ");
			return;
		}

		// send out the message
		final Message msg = new Message(Message.TYPE.SUBMIT);
		msg.setMsg(input.substring(input.indexOf(" ") + 1));
		msg.setId("Master");
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Message responseMsg = Message.sendRequest(
							config.getMasterAddr(), config.getMasterPort(), 0,
							msg);
					if (responseMsg.getType() == Message.TYPE.SUBMIT) {
						System.out.println(responseMsg.getMsg());
						System.out.print("$ ");
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
		Message responseMsg;
		try {
			responseMsg = Message.sendRequest(config.getMasterAddr(),
					config.getMasterPort(), 5000, msg);
			System.out.println(responseMsg.getMsg());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void dealWithStopCmd() {
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

	private static void dealWithMonitorCmd() {
		Message msg = new Message(Message.TYPE.MONITOR);
		msg.setMsg("Master");
		try {
			Message responseMsg = Message.sendRequest(config.getMasterAddr(),
					config.getMasterPort(), 5000, msg);
			System.out.println(responseMsg.getMsg());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws IOException {
		if (args.length != 1) {
			System.out.println("Usage: Master <Properties File>");
			System.exit(0);
		}
		config = new Config(args[0]);
		master = new Master(config);
		MasterServer server = new MasterServer(master, config);
		server.start();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));
		while (true) {
			System.out.print("$ ");
			String input = reader.readLine();
			if (input == null)
				continue;
			String[] cmdLine = input.split(" ");
			switch (cmdLine[0]) {
			case "submit":
				dealWithSubmitCmd(cmdLine, input);
				break;

			case "start":
				dealWithStartCmd();
				break;

			case "stop":
				dealWithStopCmd();
				break;

			case "monitor":
				dealWithMonitorCmd();
				break;

			case "quit":
				server.stopServer();
				master.shutdown();
				System.exit(0);
				break;

			default:
				System.out.println("Your command is not supported!");
			}
		}
	}
}
