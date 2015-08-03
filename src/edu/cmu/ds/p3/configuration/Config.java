package edu.cmu.ds.p3.configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * parse the configuration properties file
 */
public class Config {

	private Properties prop = new Properties();

	private Map<String, String> slaveAddrMap = new ConcurrentHashMap<String, String>();

	private Map<String, Integer> slavePortMap = new ConcurrentHashMap<String, Integer>();
	
    private Map<String, Integer> slaveInputFileTranPortMap = new ConcurrentHashMap<String, Integer>();
	
	private Map<String, Integer> slaveInterResTranPortMap = new ConcurrentHashMap<String, Integer>();


	public Config(String configFile) throws FileNotFoundException, IOException {		
		prop.load(new FileInputStream(configFile));
		String[] slaves = prop.getProperty("slaves").split(",");
		for (int i = 0; i < slaves.length; i++) {
			slaveAddrMap.put(slaves[i],
					InetAddress.getByName(prop.getProperty(slaves[i])).getHostAddress());
			slavePortMap.put(slaves[i], Integer.parseInt(prop.getProperty(slaves[i] + "_port")));
		
			slaveInputFileTranPortMap.put(slaves[i], Integer.parseInt(prop
					.getProperty(slaves[i] + "_inputFileTranPort")));
			slaveInterResTranPortMap.put(slaves[i], Integer.parseInt(prop
					.getProperty(slaves[i] + "_interResFileTranPort")));
		}	    
	}

	public int getMaxMaps() {
		return Integer.parseInt(prop.getProperty("max_maps"));
	}

	public int getMaxReduces() {
		return Integer.parseInt(prop.getProperty("max_reduces"));
	}
	
	public int getBufferSize() {
		return Integer.parseInt(prop.getProperty("buffer_size"));
	}

	public String getMasterAddr() throws UnknownHostException {	
		return InetAddress.getByName(prop.getProperty("master"))
					.getHostAddress();		
	}

	public int getMasterPort() {
		return Integer.parseInt(prop.getProperty("master_port"));
	}

	public Map<String, String> getSlaveAddrs() {
		return slaveAddrMap;
	}

	public String getSlaveAddr(String slaveID) {
		return getSlaveAddrs().get(slaveID);
	}

	public Map<String, Integer> getSlavePorts() {
		return slavePortMap;
	}

	public int getSlavePort(String slaveID) {
		return getSlavePorts().get(slaveID);
	}
	
	public Map<String, Integer> getClientInputFileTranPorts() {
		return slaveInputFileTranPortMap;
	}
	
	public Map<String, Integer> getClientInterResTranPorts() {
		return slaveInterResTranPortMap;
	}
	
	public int getClientInputFileTranPort(String clientID) {
		return getClientInputFileTranPorts().get(clientID);
	}
	
	public int getClientInterResTranPort(String clientID) {
		return getClientInterResTranPorts().get(clientID);
	}

}
