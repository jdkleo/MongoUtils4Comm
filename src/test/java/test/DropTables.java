package test;

import java.net.UnknownHostException;
import java.util.Set;

import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import com.odianyun.architecture.mongo.Mongopool;

public class DropTables {
	@SuppressWarnings("deprecation")
	@Test
	public void deleteTable() {
		try {
			MongoClient mc = Mongopool.getMongoClient("173.1.3.102:27017","test", null, null);
			DB db = mc.getDB("test");
			DBCollection dc = db.getCollection("data");
			dc.findOne();
			Set<String> tables = db.getCollectionNames();
			for (String name : tables) {
				if (name.indexOf("system.") < 0 && name.indexOf("fs.") < 0) {
					db.getCollection(name).drop();
				}
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
