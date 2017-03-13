package com.odianyun.architecture.mongo;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

public class MongoConnectionFactory {
	private final static String DEFAULT_HOST = "127.0.0.1";
	private final static int DEFAULT_PORT = 27017;
	private final static String DEFAULT_DB = "local";
	private final static String DEFAULT_TABLE = "test";

	public static MongoDao getDao() {
		return getDao(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_DB, DEFAULT_TABLE);
	}

	public static MongoDao getDao(String host, int port, String db) {
		return getDao(host, port, db, null);
	}

	public static MongoDao getDao(List<Map<String, Integer>> list, String db) {
		return getDao(list, db, null);
	}

	public static MongoDao getDao(String host, int port, String db, String table) {
		try {
			return new MongoDao(host, port, db, table);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static MongoDao getDao(String host, String user, String pwd, String dbname) {
		return getDao(host, user, pwd, dbname, null);
	}

	public static MongoDao getDao(String host, String user, String pwd, String dbname, String collectionName) {
		try {
			return new MongoDao(host, user, pwd, dbname, collectionName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static MongoDao getDao(List<Map<String, Integer>> list, String db, String table) {
		try {
			return new MongoDao(list, db, table);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}
}