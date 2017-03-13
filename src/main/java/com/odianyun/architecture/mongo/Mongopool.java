package com.odianyun.architecture.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class Mongopool {
	private volatile static Mongopool mongpool = null;

	// private static MongoClient mongoClient = null;
	private Mongopool() {
	}

	private static final Map<String, MongoClient> POOL_MAP = new HashMap<String, MongoClient>();
	private static final Lock LOCK = new ReentrantLock();

	/**
	 * 
	 * @param addr
	 * @param port
	 * @return
	 * @throws UnknownHostException
	 */
	public static MongoClient getMongoClient(String addr, int port) throws UnknownHostException {
		String key = addr + ":" + port;
		if (!POOL_MAP.containsKey(key)) {
			LOCK.lock();
			try {
				if (!POOL_MAP.containsKey(key)) {
					if (mongpool == null) {
						mongpool = new Mongopool();
					}
					MongoClient mc = new MongoClient(addr, port);
					mc.setWriteConcern(WriteConcern.SAFE);
					POOL_MAP.put(key, mc);
				}
			} finally {
				LOCK.unlock();
			}
		}
		return POOL_MAP.get(key);
	}

	/**
	 * 
	 * @param serverList
	 * @return
	 * @throws UnknownHostException
	 */
	public static MongoClient getMongoClient(List<Map<String, Integer>> serverList) throws UnknownHostException {
		// Builder builder=MongoClientOptions.builder();
		// MongoClientURI uri=new MongoClientURI("", builder);
		// new MongoClient(uri);
		List<ServerAddress> addrList = getAddressList(serverList);
		String key = addrList.toString();
		if (!POOL_MAP.containsKey(key)) {
			LOCK.lock();
			try {
				if (!POOL_MAP.containsKey(key)) {
					if (mongpool == null) {
						mongpool = new Mongopool();
					}
					MongoClient mc = new MongoClient(addrList);
					mc.setWriteConcern(WriteConcern.SAFE);
					POOL_MAP.put(key, mc);
				}
			} finally {
				LOCK.unlock();
			}
		}
		return POOL_MAP.get(key);
	}

	public static MongoClient getMongoClient(String host, String db, String user, String pwd) throws UnknownHostException{
		return getMongoClient(host,db,user,pwd,1,1);
	}
	/**
	 * 
	 * @param host
	 * @param db
	 * @param user
	 * @param pwd
	 * @param minConnectionsPerHost
	 * @return
	 * @throws UnknownHostException
	 */
	public static MongoClient getMongoClient(String host, String db, String user, String pwd, int minConnectionsPerHost, int maxConnectionsPerHost) throws UnknownHostException {
		Builder builder = MongoClientOptions.builder();
		builder.socketKeepAlive(true);// 是否保持长链接
		builder.minConnectionsPerHost(minConnectionsPerHost);
		builder.connectionsPerHost(maxConnectionsPerHost);
		// builder.maxWaitTime(2000);
		MongoClientURI uri = new MongoClientURI(getMongoUri(host, db, user, pwd), builder);
		String key = uri.toString();
		if (!POOL_MAP.containsKey(key)) {
			LOCK.lock();
			try {
				if (!POOL_MAP.containsKey(key)) {
					if (mongpool == null) {
						mongpool = new Mongopool();
					}
					POOL_MAP.put(key, new MongoClient(uri));
				}
			} finally {
				LOCK.unlock();
			}
		}
		return POOL_MAP.get(key);
	}

	private static String getMongoUri(String host, String db, String user, String pwd) {
		StringBuilder sb = new StringBuilder("mongodb://");
		if (user != null && pwd != null) {
			sb.append(user + ":" + pwd + "@");
		}
		sb.append(host);
		sb.append("/" + db);
		return sb.toString();
	}

	private static List<ServerAddress> getAddressList(List<Map<String, Integer>> serverList) throws UnknownHostException {
		List<ServerAddress> addrList = new ArrayList<ServerAddress>();
		for (Map<String, Integer> serverMap : serverList) {
			for (String key : serverMap.keySet()) {
				addrList.add(new ServerAddress(key, serverMap.get(key)));
			}
		}
		return addrList;
	}
}
