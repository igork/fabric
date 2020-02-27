package com.pwc.genome.database;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

//import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.eq;

//import static com.mongodb.client.model.Projections.*;
//import static com.mongodb.client.model.Sorts.descending;

/*
manually
	mongod --config /usr/local/etc/mongod.conf
 */

public class MongoServer {
	
	static String url = IMongoConstants.LOCAL_URL;
	static MongoClient mongoClient;
	static MongoDatabase database;
	static MongoCollection<Document> collection;
	
	
	public static void init(String newUrl){
		if (newUrl!=null && !newUrl.isEmpty()) {
			url = newUrl;
		}
		init();
	}
	public static void init() {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);


		MongoClientURI uri = new MongoClientURI(url);
           //"mongodb+srv://user:user@cluster0-ddiwx.mongodb.net/test?retryWrites=true&w=majority");

		mongoClient = new MongoClient(uri);
		database = mongoClient.getDatabase(IMongoConstants.DATABASE);
		collection = database.getCollection(IMongoConstants.COLLECTION);


	}
	
	public static void save(Map<String,String> map) {
        Document doc = new Document("_id",new ObjectId());
        
        doc.putAll(map);

        Date today = new Date();
        doc.put("timestamp", today.getTime());
        doc.put("time",today.toString());

        collection.insertOne(doc);

	}

	public static void saveInThread(Map<String,String> map){

		new Thread(() -> {
			save(map);
		}).start();
	}
/*
https://kb.objectrocket.com/mongo-db/how-to-sort-mongodb-query-results-using-java-387
 */
	public static List<String> findByUserIdLimited(String userid,String orderBy,int limit){


		FindIterable<Document> itCollection;

		if (userid!=null) {

			BasicDBObject order = new BasicDBObject(orderBy,-1);  //-1 desending 1-asending

			itCollection = collection.find(eq("userid", userid)).sort(order).limit(limit);  //gte
		} else {
			itCollection = collection.find().limit(limit);


		}
		List<Document> list = (List<Document>) itCollection.into(new ArrayList<Document>());

		List<String> list2 = list.stream()
				.map(Document::toJson)
				.collect(Collectors.toList());

		return list2;
	}

	public static List<String> findByUserId(String userid){
		FindIterable<Document> itCollection;

		if (userid!=null) {
			itCollection = collection.find(eq("userid", userid));  //gte
		} else {
			itCollection = collection.find();
		}
		List<Document> list = (List<Document>) itCollection.into(new ArrayList<Document>());

		List<String> list2 = list.stream()
				.map(Document::toJson)
				.collect(Collectors.toList());


		/*
		int num = 0;
		for(Document per:list){
			System.out.println(num++ + ". " + per.toJson());

		}

		for(String str:list2){
			System.out.println(str);
		}

		 */
		return list2;
	}

	public static List<String> getListOfCollections(){
		List<String> list = new ArrayList<String>();

		try {
			for (String name : database.listCollectionNames()) {
				list.add(name);
			}
		} catch (Exception e ) {
			e.printStackTrace();
		}

		return list;
	}
	
	public static void close() {
		if (mongoClient!=null)
			mongoClient.close();
		
	}

	public static void oldTest(){



        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);

        MongoClientURI uri = new MongoClientURI(IMongoConstants.LOCAL_URL);
                //"mongodb+srv://user:user@cluster0-ddiwx.mongodb.net/test?retryWrites=true&w=majority");

        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase(IMongoConstants.DATABASE);

        System.out.println(uri.toString());
        
        try {
        //MongoDatabase database = mongoClient.getDatabase("test");
	        for (String name : database.listCollectionNames()) {
	            System.out.println(name);
	        }
        } catch (Exception e ) {
        	e.printStackTrace();
        }

        MongoCollection<Document> coll = null;
        
        try {
	        coll = database.getCollection(IMongoConstants.COLLECTION);
	
	        Document doc = new Document("_id",new ObjectId());
	        //BasicDBObject doc= new BasicDBObject();
	        doc.put("user","java program");
	        doc.put("time",(new Date()).toString());
	        doc.put("userid",100);
	
	        
	        //String json = "{'json':'yes'}";
	
	        coll.insertOne(doc);
        } catch (Exception e) {
        	e.printStackTrace();
        }

        //mongodb/blog/post/quick-start-java-and-mongodb
        if (coll!=null) {
	        try {
	        	
		        FindIterable<Document> itTesters = coll.find(eq("userid",100));//.into(new ArrayList<>());
		        List<Document> list = (List<Document>) itTesters.into(new ArrayList<Document>());
		
		        int num = 0;
		        for(Document per:list){
		            System.out.println(num++ + ". " + per.toJson());
		        }
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }
        }
        mongoClient.close();

    }

    public static void newTest(String arg){

		Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);


		MongoServer.init(arg);
		System.out.println(MongoServer.mongoClient.getConnectPoint());

		List<String> list = MongoServer.getListOfCollections();
		System.out.println("collections: " + Arrays.toString(list.toArray()));

		Map<String,String> doc = new HashMap<String,String>();
		doc.put("user","java program");
		doc.put("time",(new Date()).toString());
		doc.put("userid","" + 100);
		MongoServer.save(doc);

		String userid="100";
		list = MongoServer.findByUserIdLimited(userid,"timestamp",3);
		System.out.println("find by userid: " + userid);
		int i=0;
		for (String record : list){
			System.out.println(i++ + ".  " +record);
		}

		userid = "pulsar";
		list = MongoServer.findByUserId(userid);
		System.out.println("find by userid: " + userid);
		i=0;
		for (String record : list){
			System.out.println(i++ + ".  " +record);
		}

		MongoServer.close();


	}


	public static void main(String[] arg){
		//oldTest();

		if (arg!=null && arg.length>0) {

			newTest(arg[0]);

		} else {
			newTest(null);
		}

	}

}
