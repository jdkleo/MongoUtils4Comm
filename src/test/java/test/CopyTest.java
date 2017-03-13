package test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.odianyun.architecture.mongo.MongoConnectionFactory;
import com.odianyun.architecture.mongo.MongoDao;

public class CopyTest {
	@Test
	public void copytest(){
		try {
			MongoDao md = MongoConnectionFactory.getDao("192.168.20.169:27017", "", "", "chae");
			List<String> query = new ArrayList<String>();
			query.add("AND");
			query.add("orderDate");
			query.add("=");
			query.add("2016-09-12");

			OrderCountNumDTO obj = new OrderCountNumDTO();
			obj.setOrderDate("2016-09-12");
			obj.setCountNum(10);
			md.upsertOne(query, obj);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
