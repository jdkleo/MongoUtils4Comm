package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;

import com.odianyun.architecture.mongo.MongoConnectionFactory;
import com.odianyun.architecture.mongo.MongoDao;

public class TestCase {
//	 @Test
	public void copyIndexTest() {
		MongoDao mc = MongoConnectionFactory.getDao();
		long s = System.currentTimeMillis();
		mc.copyIndex("test", "aa");
		System.out.println(System.currentTimeMillis() - s);
	}

//	 @Test
	public void copytest() {
		MongoDao mc = MongoConnectionFactory.getDao();
		long s = System.currentTimeMillis();
		List<String> query = new ArrayList<String>();
		query.add("AND");
		query.add("stock_date");
		query.add("=");
		query.add("2014-08-04");
		query.add("source");
		query.add("=");
		query.add("0");
		Double d = mc.copyTable("test", "aa", query);
		System.out.println(d.equals(new Double(0)) + "  " + d);
		System.out.println(System.currentTimeMillis() - s);
	}

//	@Test
	public void inserttest() {
		MongoDao mc = MongoConnectionFactory.getDao("10.32.8.22", 28018, "local", "test");
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("aaa", "aaa");
		map.put("bbb", 1);
		map.put("ccc", 4.2);
		mc.insertOne(map);
		for (Map<String, Object> dbo : mc.queryMapList(null)) {
			System.out.println(dbo);
		}
	}

//	 @Test
	public void querytest() {
		MongoDao mc = MongoConnectionFactory.getDao("10.32.8.22", 28018, "local", "test");
		long s = System.currentTimeMillis();
		List<String> query = new ArrayList<String>();
		System.out.println(mc.getDbCollection());
		query.add("AND");
		query.add("stock_date");
		query.add("=");
		query.add("2014-08-04");
		query.add("source");
		query.add("=");
		query.add("1");
		// query.add("AND");
		// query.add("_id");
		// query.add("=");
		// query.add("5450aa60300d274b7705cc9d");
		// List<Map<String, Object>> d = mc.queryMapList(null);
		Map<String, Object> dbo = mc.queryMapOne(query);
		System.out.println(dbo);
		List<Map<String, Object>> list = mc.queryMapList(query);
		for (Map<String, Object> map : list) {
			System.out.println(map);
		}
		// System.out.println(dbo);
		// dbo=new HashMap<String, String>();
		// ObjectId oid=new ObjectId("54440c41e4b0e2fe1b8b379a");
		// dbo.put("_id", oid);
		// dbo.put("especiallystatus", "busb");
		// // dbo.remove("especiallystatus");
		// // dbo.remove("expirydate");
		// mc.getDbCollection().save(new BasicDBObject(dbo));
		// System.out.println(d.equals(new Double(0)) + "  " + d);
		System.out.println(System.currentTimeMillis() - s);
	}

	 @Test
	public void maprtest() {
		MongoDao mc = MongoConnectionFactory.getDao("10.32.7.28", 27017, "savemongo", "sic_contrast_0_20151017_139_1");
		String mapfun = "function Map() {emit( this.key,{type:this.sourcesys, qty: this.goodsnum,qty1:0,qty2:0,qty0:0,res1:'0',res0:'0'} ); }";
		String reducefun = "function Reduce(key, values) {var reduced = {type:'',qty:0,qty1:0,qty2:0,qty0:0,res1:'0',res0:'0'};values.forEach(function(val){if(val.type=='1'){   reduced.qty1+=val.qty; }  if(val.type=='2'){   reduced.qty2+=val.qty;}  if (val.type=='0'){   reduced.qty0+=val.qty; }reduced.qty2+=val.qty2;  reduced.qty0+=val.qty0;reduced.qty1+=val.qty1;}); return reduced;}";
		String finalizefun = "function Finalize(key, reduced) {if(reduced.type=='1'){ reduced.qty1+=reduced.qty; }if(reduced.type=='2'){ reduced.qty2+=reduced.qty;}if (reduced.type=='0'){ reduced.qty0+=reduced.qty; }reduced.res1 = reduced.qty2-reduced.qty1;reduced.res0 = reduced.qty2-reduced.qty0;return reduced;}";
		Map<String, String> outMap = new HashMap<String, String>();
		outMap.put("replace", "res");
		mc.mapreduce(mapfun, reducefun, finalizefun, null, outMap, "key");
	}

	// @Test
	// public void threadTest() {
	// for (int j = 0; j < 1; j++) {
	// Thread mt = new Thread(new MyThread(), "" + j);
	// mt.start();
	// }
	// try {
	// Thread.sleep(0);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }

	// class MyThread implements Runnable {
	//
	// @Override
	// public void run() {
	// MongoDao mc = MongoConnectionFactory.getDao("10.32.8.22", 28018,
	// "test", "nc_save_Logger");
	// long s = System.currentTimeMillis();
	// List<String> query = new ArrayList<String>();
	// query.add("AND");
	// query.add("logdate");
	// query.add("=");
	// query.add("2014-10-13");
	// while (true) {
	// System.out.println(mc.getDbCollection());
	// long d = mc.count();
	// System.out.println(d);
	// mc = MongoConnectionFactory.getDao("10.32.8.22", 28018,
	// "test", "nc_save_Logger");
	// System.out.println(mc.getDbCollection() + " while");
	// }
	//
	// }
	// }

	// @Test
	public void getDaotest() {
		List<Map<String, Integer>> servl = new ArrayList<Map<String, Integer>>();
		Map<String, Integer> m = new HashMap<String, Integer>();
		m.put("10.32.8.16", 28018);
		m.put("10.32.8.17", 28018);
		m.put("10.32.8.18", 28018);
		servl.add(m);
		MongoDao mc = MongoConnectionFactory.getDao(servl, "test", "test");
		System.out.println(mc.getDbCollection());
		System.out.println(mc);
	}

	// @Test
	@SuppressWarnings("resource")
	public void getDaoTest2() {
		Builder builder = MongoClientOptions.builder();
		builder.minConnectionsPerHost(100);
		// builder.heartbeatConnectRetryFrequency(20);
		// "minConnectionsPerHost=100";
		String u = getMongoUri("10.32.8.16:28018,10.32.8.17:28018", "test", null, null, "connectionsPerHost=50");
		MongoClientURI uri = new MongoClientURI(u);
		System.out.println(uri.toString());
		MongoClient mc = null;
		mc = new MongoClient(uri);
		System.out.println(mc.getMongoClientOptions());
		// try {
		// MongoClient md = Mongopool.getMongoClient(
		// "10.32.8.16:28018,10.32.8.17:28018", "test", null, null,
		// 100);
		// md.getDatabaseNames();
		// System.out.println(md);
		// } catch (UnknownHostException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }
	}

	private String getMongoUri(String host, String db, String user, String pwd, String option) {
		StringBuffer sb = new StringBuffer();
		sb.append("mongodb://");
		if (user != null && pwd != null) {
			sb.append(user + ":" + pwd + "@");
		}
		sb.append(host);
		sb.append("/" + db);
		if (option != null && !"".equals(option)) {
			sb.append("?" + option);
		}
		return sb.toString();
	}

}
