package com.odianyun.architecture.mongo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.mongodb.MongoClient;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;
import com.odianyun.architecture.mongo.dto.MongoConnDTO;
import org.bson.*;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

public class MongoDao {
	private MongoClient mongoClient;
	private MongoDatabase mdb;
	private MongoCollection<Document> dbCollection;
	private static Map<String, String> conditionsMap = new HashMap<String, String>();
	private final static Logger logger = LoggerFactory.getLogger(MongoDao.class);
	static {
		conditionsMap.put("=", "$eq");
		conditionsMap.put(">", "$gt");
		conditionsMap.put("<", "$lt");
		conditionsMap.put("<=", "$lte");
		conditionsMap.put(">=", "$gte");
		conditionsMap.put("!=", "$ne");
		conditionsMap.put("contain", "$regex");
		conditionsMap.put("regex", "$regex");
		conditionsMap.put("AND", "$and");
		conditionsMap.put("OR", "$or");
		conditionsMap.put("and", "$and");
		conditionsMap.put("or", "$or");
	}
	private final UpdateOptions _UPDATEOPTIONS_TRUE = new UpdateOptions().upsert(true);



	private static enum UPDATE_TYPE{
		UPDATE_ONE,UPDATE,UPSERT_ONE,UPSERT
	}


	public MongoDao(MongoConnDTO mongoConnDTO, String collectionName) throws UnknownHostException {
		this(mongoConnDTO.getHost(), mongoConnDTO.getUser(), mongoConnDTO.getPwd(), mongoConnDTO.getDbname(),mongoConnDTO.getMinConn(), mongoConnDTO.getMaxConn(), collectionName);
	}

	public MongoDao(String host, String user, String pwd, String dbname, String collectionName) throws UnknownHostException {
		this(host, user, pwd, dbname,1, 1, collectionName);
	}

	public MongoDao(String host, String user, String pwd, String dbname,int minConn, int maxConn, String collectionName) throws UnknownHostException {
		mongoClient = Mongopool.getMongoClient(host, dbname, user, pwd, minConn, maxConn);
		mdb = mongoClient.getDatabase(dbname);
		if (collectionName != null && !"".equals(collectionName)) {
			dbCollection = mdb.getCollection(collectionName);
		}
	}

	public MongoDao(List<Map<String, Integer>> serverList, String dbname, String collectionName) throws UnknownHostException {
		mongoClient = Mongopool.getMongoClient(serverList);
		mdb = mongoClient.getDatabase(dbname);
		if (!"".equals(collectionName)) {
			dbCollection = mdb.getCollection(collectionName);
		}
	}

	public MongoDao(String addr, int port, String dbname, String collectionName) throws UnknownHostException {
		mongoClient = Mongopool.getMongoClient(addr, port);
		mdb = mongoClient.getDatabase(dbname);
		if (!"".equals(collectionName)) {
			dbCollection = mdb.getCollection(collectionName);
		}
	}

	private Map<String, Object> toMap(Document document) {
		return JSONObject.parseObject(document.toJson(), new TypeReference<Map<String, Object>>() {
		});
	}

	public void rename(String oColName, String newName) {
		mdb.getCollection(oColName).renameCollection(new MongoNamespace(mdb.getName(), newName));
	}

	public Double copyTable(String oTableName, String nTableName, List<?> queryCond) {
		String queryString = "";
		if (queryCond != null && queryCond.size() != 0) {
			BsonDocument dbo = getQueryCond(queryCond);
			queryString = dbo.toJson().replaceAll("\"", "'");
		}
		String cmd = "function(){db." + oTableName + ".find(" + queryString + ").forEach(function(x){db." + nTableName + ".insert(x)}); return db." + nTableName + ".count();}";
		Object[] args = { oTableName, nTableName };
		Bson bson = BsonDocument.parse("{ eval : \"" + cmd + "\" }, args : " + com.alibaba.fastjson.JSON.toJSONString(args) + "  }");
		Document d = mdb.runCommand(bson);
		return d.getDouble("retval");
	}

	public void copyIndex(String oTableName, String nTableName) {
		MongoCollection<Document> dbcO = mdb.getCollection(oTableName);
		MongoCollection<Document> dbcN = mdb.getCollection(nTableName);
		MongoCursor<Document> cursor = dbcO.listIndexes().iterator();
		StringBuilder json = new StringBuilder();
		while (cursor.hasNext()) {
			Document doc = cursor.next();
			doc.put("ns", dbcN.getNamespace().toString());
			json.append(",").append(doc.toJson());
		}
		json.deleteCharAt(0);
		json.append("]}");
		json.insert(0, "\", indexes:[").insert(0, nTableName).insert(0, "{createIndexes: \"");
		BsonDocument cmd = BsonDocument.parse(json.toString());
		mdb.runCommand(cmd);
	}

	public MongoCollection<Document> getDbCollection() {
		return dbCollection;
	}

	public void setDbCollection(String dbCollectionName) {
		this.dbCollection = mdb.getCollection(dbCollectionName);
	}

	public <T> void insertOne(T obj) {
		if (obj instanceof Document) {
			dbCollection.insertOne((Document) obj);
		} else {
			insertOne(Bean2Document(obj));
		}
	}


	public <T> void insertList(List<T> list) {
		for (T e : list) {
			insertOne(e);
		}
	}

	public void insertOne(String json) {
		dbCollection.insertOne(Document.parse(json));
	}

	public void insertOne(Map<String, Object> map) {
		dbCollection.insertOne(new Document(map));
	}


	public <T> UpdateResult updateOne(List<?> query, T obj){
		return upOption(query,obj,UPDATE_TYPE.UPDATE_ONE);
	}

	public <T> UpdateResult update(List<?> query, T obj){
		return upOption(query,obj,UPDATE_TYPE.UPDATE);
	}

	public <T> UpdateResult upsertOne(List<?> query, T obj){
		return upOption(query,obj,UPDATE_TYPE.UPSERT_ONE);
	}

	public <T> UpdateResult upsert(List<?> query, T obj){
		return upOption(query,obj,UPDATE_TYPE.UPSERT);
	}

	public <T> UpdateResult updateAndIncOne(List<?> query, T obj, String... incFields){
		return upIncOption(query,obj,incFields,UPDATE_TYPE.UPDATE_ONE);
	}

	public <T> UpdateResult updateAndInc(List<?> query, T obj, String... incFields){
		return upIncOption(query,obj,incFields,UPDATE_TYPE.UPDATE);
	}

	public <T> UpdateResult upsertAndIncOne(List<?> query, T obj, String... incFields){
		return upIncOption(query,obj,incFields,UPDATE_TYPE.UPSERT_ONE);
	}

	public <T> UpdateResult upsertAndInc(List<?> query, T obj, String... incFields){
		return upIncOption(query,obj,incFields,UPDATE_TYPE.UPSERT);
	}

	private <T> UpdateResult upOption(List<?> query, T obj, UPDATE_TYPE upType) {
		Document document = (obj instanceof Document) ? (Document)obj : Bean2Document4Update(obj);
		BsonDocument condition = getQueryCond(query);
		return updateOption(condition,document,upType);
	}

	private <T> UpdateResult upIncOption(List<?> query, T obj, String[] incFields, UPDATE_TYPE upType) {
		Document document = (obj instanceof Document) ? (Document)obj : Bean2Document4UpdateInc(obj,incFields);
		BsonDocument condition = getQueryCond(query);
		return updateOption(condition,document,upType);
	}

	private UpdateResult updateOption(BsonDocument condition, Document document, UPDATE_TYPE upType) {
		switch(upType){
			case UPDATE_ONE:
				return dbCollection.updateOne(condition,document);
			case UPDATE:
				return dbCollection.updateMany(condition,document);
			case UPSERT_ONE:
				return dbCollection.updateOne(condition,document,_UPDATEOPTIONS_TRUE);
			case UPSERT:
				return dbCollection.updateMany(condition,document,_UPDATEOPTIONS_TRUE);
		}
		return null;
	}


	private long count(BsonDocument dbo) {
		return dbCollection.count(dbo);
	}

	public long count() {
		return dbCollection.count();
	}

	public long count(Map<String, Object> map) {
		return count(new Document(map));
	}

	public long count(List<?> list) {
		if (list == null || list.size() == 0) {
			return count();
		} else {
			return count((BsonDocument) getQueryCond(list));
		}
	}

	public <T> List<T> queryList(List<?> list, Class<T> clz) {
		return queryList(list, clz, null);
	}
	
	public <T> List<T> queryList(List<?> list, Class<T> clz, LinkedHashMap<String, Integer> sort) {
		return queryList(getQueryCond(list), clz, sort);
	}
	
	private <T> List<T> queryList(BsonDocument doc, Class<T> clz, LinkedHashMap<String, Integer> sort) {
		MongoCursor<Document> cursor = dbCollection.find(doc).sort(getSort(sort)).iterator();
		return cursor2List(cursor,clz);
	}

	private <T> List<T> cursor2List(MongoCursor<Document> cursor, Class<T> clz) {
		List<T> resList = new ArrayList<T>();
		try {
			while (cursor.hasNext()) {
				resList.add(json2Vo(cursor.next().toJson(), clz));
			}
		} catch(Throwable t){
			logger.error(t.getMessage(),t);
		}finally {
			if (cursor != null)
				cursor.close();
		}
		return resList;
	}

	public <T> List<T> queryListByPage(List<?> list, Class<T> clz, LinkedHashMap<String, Integer> sort,int skip,int limit) {
		return queryListByPage(getQueryCond(list), clz, sort,skip,limit);
	}
	
	private <T> List<T> queryListByPage(BsonDocument doc, Class<T> clz, LinkedHashMap<String, Integer> sort, int skip,int limit) {
		MongoCursor<Document> cursor = dbCollection.find(doc).sort(getSort(sort)).skip(skip).limit(limit).iterator();
		return cursor2List(cursor,clz);
	}

	public List<Map<String, Object>> queryMapList(List<?> list) {
		return queryMapList(list, null);
	}

	public List<Map<String, Object>> queryMapList(List<?> list, LinkedHashMap<String, Integer> sort) {
		List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
		MongoCursor<Document> dbCur = null;
		try {
			// TODO 优化
			dbCur = dbCollection.find(getQueryCond(list)).sort(getSort(sort)).iterator();
			while (dbCur.hasNext()) {
				Document res = dbCur.next();
				resList.add(toMap(res));
			}
		}catch(Throwable t){
			logger.error(t.getMessage(),t);
		} finally {
			if (dbCur != null)
				dbCur.close();
		}
		return resList;
	}
	
	public List<Map<String, Object>> queryMapListByPage(List<?> list, LinkedHashMap<String, Integer> sort,int skip,int limit) {
		List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
		MongoCursor<Document> dbCur = null;
		try {
			// TODO 优化
			dbCur = dbCollection.find(getQueryCond(list)).sort(getSort(sort)).skip(skip).limit(limit).iterator();
			while (dbCur.hasNext()) {
				Document res = dbCur.next();
				resList.add(toMap(res));
			}
		}catch(Throwable t){
			logger.error(t.getMessage(),t);
		} finally {
			if (dbCur != null)
				dbCur.close();
		}
		return resList;
	}

	private Document getSort(Map<String, Integer> sort) {
		if (sort == null) {
			return null;
		}
		if (sort.size() == 0) {
			return null;
		}
		Document sortList = new Document();
		for (String key : sort.keySet()) {
			if (sort.get(key) <= 0) {
				sortList.append(key, -1);
			} else {
				sortList.append(key, 1);
			}
		}
		return sortList;
	}

	public <T> List<T> queryJson(String json, Class<T> clz) {
		return queryJson(json, clz, null);
	}

	public <T> List<T> queryJson(String json, Class<T> clz, LinkedHashMap<String, Integer> sort) {
		List<T> resList = new ArrayList<T>();
		Document ref = Document.parse(json);
		MongoCursor<Document> dbCur = null;
		try {
			dbCur = dbCollection.find(ref).sort(getSort(sort)).iterator();
			while (dbCur.hasNext()) {
				Document res = dbCur.next();
				T t = json2Vo(res.toString(), clz);
				resList.add(t);
			}
		}catch(Throwable t){
			logger.error(t.getMessage(),t);
		} finally {
			if (dbCur != null)
				dbCur.close();
		}
		return resList;
	}

	public String queryJsonByJson(String json) {
		return queryJsonByJson(json, null);
	}

	public String queryJsonByJson(String json, LinkedHashMap<String, Integer> sort) {
		List<Map<String, Object>> resList = new ArrayList<Map<String, Object>>();
		Document ref = Document.parse(json);
		MongoCursor<Document> dbCur = null;
		try {
			dbCur = dbCollection.find(ref).sort(getSort(sort)).iterator();
			while (dbCur.hasNext()) {
				Document res = dbCur.next();
				resList.add(toMap(res));
			}
		} catch(Throwable t){
			logger.error(t.getMessage(),t);
		}finally {
			if (dbCur != null)
				dbCur.close();
		}
		return JSON.serialize(resList);
	}

	private static <T> Document Bean2Document4Update(T obj) {
		Map<String,Object> map = new HashMap<String, Object>();
		map.put("$set",obj);
		return Document.parse(JSONObject.toJSONString(map, SerializerFeature.WriteDateUseDateFormat));
	}

	private static <T> Document Bean2Document4UpdateInc(T obj, String... incFields) {
		JSONObject ojson = (JSONObject)JSONObject.toJSON(obj);
		Map<String,Object> incMap = new HashMap<String, Object>();
		for (String incField : incFields) {
			incMap.put(incField, ojson.get(incField));
			ojson.remove(incField);
		}
		Map<String,Object> map = new HashMap<String, Object>();
		map.put("$set",ojson);
		if (!incMap.isEmpty()) {
			map.put("$inc", incMap);
		}
		Document document = Document.parse(JSONObject.toJSONString(map, SerializerFeature.WriteDateUseDateFormat));
		if (((Document)document.get("$set")).isEmpty()){
			document.remove("$set");
		}
		return document;
	}

	private static <T> Document Bean2Document(T obj) {
		return Document.parse(JSONObject.toJSONString(obj, SerializerFeature.WriteDateUseDateFormat));
	}

	private <T> T json2Vo(String json, Class<T> clz) {
		return (T) JSONObject.parseObject(json, clz);
	}

	private BsonDocument getQueryCond(List<?> list) {
		BsonDocument document = new BsonDocument();
		if (list == null) {
			return document;
		}
		if (list.size() == 0) {
			return document;
		}
		String conditions;
		int size = list.size();
		Object connector = list.get(0);
		if (connector == null){
			throw new IllegalArgumentException("the first condition must be 'AND' or 'OR'");
		}
		String conditionHead = connector.toString().toUpperCase();
		if ("AND".equals(conditionHead) || "OR".equals(conditionHead)) {
			conditions = conditionsMap.get(conditionHead);
		} else {
			throw new IllegalArgumentException("the first condition must be 'AND' or 'OR'");
		}
		if (size == 4) {
			document = getCond(list, 1);
		} else {
			BsonArray docList = new BsonArray();
			for (int i = 1; i * 3 < size; i++) {
				BsonDocument doc = getCond(list, i);
				if (doc != null) {
					docList.add(doc);
				}
			}
			document.put(conditions, docList);
		}
		return document;
	}

	private BsonDocument getCond(List<?> list, int start) {
		start = start * 3 - 2;
		BsonDocument doc = new BsonDocument();
		
		if (conditionsMap.get(list.get(start + 1)) != null) {
			doc.append((String)list.get(start), new BsonDocument(conditionsMap.get(list.get(start + 1)),getBsonValue(list.get(start + 2))));
		}
		return doc;
	}

	private BsonValue getBsonValue(Object value) {
		if (value instanceof Integer){
			return new BsonInt32((Integer)value);
		}
		if (value instanceof Long){
			return new BsonInt64((Long)value);
		}
		if (value instanceof Double){
			return new BsonDouble((Double)value);
		}
		if (value instanceof Boolean){
			return new BsonBoolean((Boolean)value);
		}
		if (value instanceof Date){
			return new BsonDateTime(((Date)value).getTime());
		}
		return new BsonString((String)value);
	}

	/**
	 * 传入WHERE条件，VO的CLASS list.get(0) 为AND||OR标识，i%3=1 为字段名 i%3=2 为关系 i%3=0
	 * 
	 * @param list
	 * @param clz
	 * @return
	 */
	public <T> T queryOne(List<?> list, Class<T> clz) {
		FindIterable<Document> iterable = dbCollection.find(getQueryCond(list));
		if (iterable == null){
			return null;
		}
		Document doc = iterable.first();
		if (doc == null || doc.isEmpty()){
			return null;
		}
		return (T) json2Vo(doc.toJson(), clz);
	}

	public Map<String, Object> queryMapOne(List<?> list) {
		FindIterable<Document> iterable = dbCollection.find(getQueryCond(list));
		if (iterable == null){
			return null;
		}
		Document doc = iterable.first();
		if (doc == null || doc.isEmpty()){
			return null;
		}
		return toMap(doc);
	}

	public void createIndex(Map<String, Integer> col, boolean unique, boolean dropDups) {
		createIndex(col, unique, dropDups, null);
	}

	public void createIndex(Map<String, Integer> col, boolean unique) {
		createIndex(col, unique, false, null);
	}

	private void createIndex(Map<String, Integer> col, boolean unique, boolean dropDups, String tName) {
		Document doc = new Document();
		MongoCollection<Document> dbc = tName == null ? dbCollection : mdb.getCollection(tName);
		doc.putAll(col);
		IndexOptions opt = new IndexOptions();
		opt.sparse(true);
		if (unique) {
			opt.unique(true);
		}
		dbc.createIndex(doc, opt);
	}

	public String mapreduce(String mapfun, String reducefun, String finalizefun, List<?> query, Map<String, String> outMap, String sortCol) {
		Document command = new Document();
		command.put("mapreduce", dbCollection.getNamespace().getCollectionName());
		if (query != null) {
			if (query.size() != 0) {
				command.put("query", getQueryCond(query));
			}
		}
		command.put("map",  mapfun );
		command.put("reduce", reducefun );
		if (finalizefun != null && !"".equals(finalizefun)) {
			command.put("finalize",finalizefun );
		}
		Document out = new Document();
		out.putAll(outMap);
		command.put("out", out);
		command.put("verbose", true);
		if (sortCol != null) {
			if (!"".equals(sortCol)) {
				Document sort = new Document();
				sort.put(sortCol, 1);
				command.put("sort", sort);
			}
		}
		command.toJson();
		Document result = mdb.runCommand(command);
		return JSON.serialize(result);
	}

	public void delete() {
		dbCollection.drop();
	}

	public void delete(List<?> query) {
		dbCollection.deleteMany(getQueryCond(query));
	}

}
