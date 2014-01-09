import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoException.DuplicateKey;


public class MongoJUnitTest {
	
	private TestMongo testMongo;


	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		testMongo = new TestMongo();
		testMongo.initClient();
	}

	@After
	public void tearDown() throws Exception {
	}

	
	@Test
	public void testBasic() {

		DB db = testMongo.getDB();

		Set<String> colls = db.getCollectionNames();
		

		System.out.println("MongoDB "+db.getName()+" collections before clean: ");
		for (String s : colls) {
		    System.out.println(s);
		}
		
		testMongo.deleteCollection(TestMongo.USERS_COLLECTION);
		testMongo.createCollection(TestMongo.USERS_COLLECTION);
		

		
		colls = db.getCollectionNames();
		

		System.out.println("MongoDB "+db.getName()+" collections after clean: ");
		for (String s : colls) {
		    System.out.println(s);
		}
		
		//create users:
		testMongo.createUsers(TestMongo.NUM_OF_USERS);
		
		//test unique
		{
			System.out.println("******");
			System.out.println("Test Unique:");
			System.out.println("******");
			try{
				Map<String, Object> user = new LinkedHashMap<String, Object>();
				user.put(TestMongo.UNIQUE_INDEX, "user-"+0);
				user.put("type", "type-"+0);
				user.put("index", 0);
				testMongo.insertDoc("users", testMongo.createDoc(user));
			}
			catch(DuplicateKey dke){
				dke.printStackTrace();
			}

		}

		
		//get users:
		{
			System.out.println("******");
			System.out.println("Get Users:");
			System.out.println("******");
			DBCollection collection = testMongo.getCollection(TestMongo.USERS_COLLECTION);
			DBCursor cursor = collection.find();
			assertTrue(cursor.count()==TestMongo.NUM_OF_USERS);
			try {
				while (cursor.hasNext()) {
					System.out.println(cursor.next());
				}
			} finally {
				cursor.close();
			}
		}
		
		//get users with type-1 only:
		{
			System.out.println("******");
			System.out.println("Users with only type-1");
			System.out.println("******");
			
			DBCollection collection = testMongo.getCollection(TestMongo.USERS_COLLECTION);
			DBCursor cursor = collection.find(new BasicDBObject("type", "type-1"));
			assertTrue(cursor.count()==TestMongo.NUM_OF_USERS/3);
			try {
			   while(cursor.hasNext()) {
			       System.out.println(cursor.next());
			   }
			} finally {
			   cursor.close();
			}
			
		}
	}

}
