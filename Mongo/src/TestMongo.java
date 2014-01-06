import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException.DuplicateKey;


public class TestMongo {
	
	public static final int NUM_OF_USERS = 10;
	protected String dbName = "test";
	public static final String USERS_COLLECTION = "users";
	public static final String MANAGER_RELATIONS_COLLECTIONS = "ManagerRelations";
	public static final String UNIQUE_INDEX = "uniqueid";
	
	protected MongoClient mongoClient = null;
	protected DB db = null;
	
	public static void main(String[] args){
		TestMongo testMongo = new TestMongo();
		testMongo.initClient();
		
		
		testBasic(testMongo);
		
		testLink(testMongo);

				
	}

	public static void testBasic(TestMongo testMongo) {

		DB db = testMongo.getDB();

		Set<String> colls = db.getCollectionNames();
		

		System.out.println("MongoDB "+db.getName()+" collections before clean: ");
		for (String s : colls) {
		    System.out.println(s);
		}
		
		testMongo.deleteCollection(USERS_COLLECTION);
		testMongo.createCollection(USERS_COLLECTION);
		

		
		colls = db.getCollectionNames();
		

		System.out.println("MongoDB "+db.getName()+" collections after clean: ");
		for (String s : colls) {
		    System.out.println(s);
		}
		
		//create users:
		for(int i=0;i<NUM_OF_USERS;i++){
			Map<String, Object> user = new LinkedHashMap<String, Object>();
			user.put(UNIQUE_INDEX, "user-"+i);
			user.put("type", "type-"+i%3);
			user.put("index", i);
			testMongo.insertDoc(USERS_COLLECTION, testMongo.createDoc(user));
		}
		
		//test unique
		{
			System.out.println("******");
			System.out.println("Test Unique:");
			System.out.println("******");
			try{
				Map<String, Object> user = new LinkedHashMap<String, Object>();
				user.put(UNIQUE_INDEX, "user-"+0);
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
			DBCollection collection = testMongo.getCollection(USERS_COLLECTION);
			DBCursor cursor = collection.find();
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
			
			DBCollection collection = testMongo.getCollection(USERS_COLLECTION);
			DBCursor cursor = collection.find(new BasicDBObject("type", "type-1"));
			try {
			   while(cursor.hasNext()) {
			       System.out.println(cursor.next());
			   }
			} finally {
			   cursor.close();
			}
			
		}
	}

	public static void testLink(TestMongo testMongo){
		DB db = testMongo.getDB();
		Set<String> colls = db.getCollectionNames();
		
		System.out.println("MongoDB "+db.getName()+" collections before clean: ");
		for (String s : colls) {
		    System.out.println(s);
		}
		
		testMongo.deleteCollection(MANAGER_RELATIONS_COLLECTIONS);
		testMongo.createCollection(MANAGER_RELATIONS_COLLECTIONS);
		
		//get user 1
		DBObject user1 = db.getCollection(USERS_COLLECTION).findOne(new BasicDBObject(UNIQUE_INDEX, "user-1"));
		DBObject user2 = db.getCollection(USERS_COLLECTION).findOne(new BasicDBObject(UNIQUE_INDEX, "user-2"));

		Map<String, Object> relationship = new LinkedHashMap<String, Object>();
		relationship.put(UNIQUE_INDEX, "rel-"+user1.get(UNIQUE_INDEX)+"-"+user2.get(UNIQUE_INDEX));
		relationship.put("from", user1.get(UNIQUE_INDEX));
		relationship.put("to", user2.get(UNIQUE_INDEX));
		relationship.put("Created", new Date());
		relationship.put("index", 0);
		testMongo.insertDoc(MANAGER_RELATIONS_COLLECTIONS, testMongo.createDoc(relationship));
		
		//get the relationship 
		{
			System.out.println("******");
			System.out.println("relationships:");
			System.out.println("******");
			
			DBCollection collection = testMongo.getCollection(MANAGER_RELATIONS_COLLECTIONS);
			DBCursor cursor = collection.find();
			try {
			   while(cursor.hasNext()) {
			       System.out.println(cursor.next());
			   }
			} finally {
			   cursor.close();
			}
			
		}
			//	
	}
	
	
	public BasicDBObject createDoc(Map<String, Object> fields){
		
		
		BasicDBObject doc = new BasicDBObject();
		for(Map.Entry<String, Object> entry : fields.entrySet()){
			
			doc.append(entry.getKey(), entry.getValue());
			
		}

		return doc;


	}
	
	public void deleteCollection(String collectionName){
		db.getCollection(collectionName).drop();
	}
	
	public void createCollection(String collectionName){
		DBCollection collection = db.createCollection(collectionName, null);
		collection.ensureIndex(new BasicDBObject(UNIQUE_INDEX, 1), null, true);
	


	}
	
	public void insertDoc(String collectionName, BasicDBObject doc ){
		getCollection(collectionName).insert(doc);
	}


	protected void initClient() {
		mongoClient = null;
		try {
			mongoClient = new MongoClient( "localhost" );
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = mongoClient.getDB(dbName);

	}
	
	public DB getDB(){
		return mongoClient.getDB( dbName);
	}
	
	public DBCollection getCollection(String colName){
		return getDB().getCollection(colName);
	}
	
	public void addCollection(String collectionName){
		
	}

}
