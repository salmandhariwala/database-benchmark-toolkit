package mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoCRUD {

	Gson gson = new Gson();

	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection;

	private Exception LastException;

	private void initMongo(String host, int port) {
		mongoClient = new MongoClient(host, port);
	}

	public MongoCRUD(HashMap<String, Integer> MongoConnectconfig) {
		initMongo(MongoConnectconfig);
	}

	public MongoCRUD(String host, int port) {
		initMongo(host, port);
	}

	private void initMongo(HashMap<String, Integer> MongoConnectconfig) {

		List<ServerAddress> list = new ArrayList<ServerAddress>();

		for (Map.Entry<String, Integer> entry : MongoConnectconfig.entrySet()) {
			String key = entry.getKey();
			Integer value = entry.getValue();
			ServerAddress sa = new ServerAddress(key, value);
			list.add(sa);
		}

		mongoClient = new MongoClient(list);
	}

	public ArrayList<HashMap<String, Object>> getDocs(String databaseName, String collectionName) {
		return getDocs(databaseName, collectionName, null);
	}

	public ArrayList<HashMap<String, Object>> getDocs(String databaseName, String collectionName,
			HashMap<String, Object> findCriteria) {

		ArrayList<HashMap<String, Object>> doclist = new ArrayList<HashMap<String, Object>>();

		database = mongoClient.getDatabase(databaseName);
		collection = database.getCollection(collectionName);
		MongoCursor<Document> cursor;

		if (findCriteria != null) {
			Document findDocument = new Document(findCriteria);
			cursor = collection.find(findDocument).iterator();
		} else {
			cursor = collection.find().iterator();
		}

		try {
			while (cursor.hasNext()) {

				Document d = cursor.next();

				HashMap<String, Object> temp = new HashMap<String, Object>();

				for (Map.Entry<String, Object> entry : d.entrySet()) {
					String key = entry.getKey();
					Object value = entry.getValue();
					temp.put(key, value);
				}

				doclist.add(temp);

			}
		} finally {
			cursor.close();
		}

		return doclist;

	}

	public boolean insertDocs(String databaseName, String collectionName, HashMap<String, Object> insertDoc) {

		database = mongoClient.getDatabase(databaseName);
		collection = database.getCollection(collectionName);

		try {

			collection.insertOne(new Document(insertDoc));

			return true;
		} catch (Exception e) {
			LastException = e;
			return false;
		}

	}

	public boolean insertDocs(String databaseName, String collectionName,
			ArrayList<HashMap<String, Object>> insertDoc) {

		database = mongoClient.getDatabase(databaseName);
		collection = database.getCollection(collectionName);

		try {

			ArrayList<Document> list = new ArrayList<Document>();

			for (HashMap<String, Object> m : insertDoc) {
				list.add(new Document(m));
			}

			collection.insertMany(list);

			return true;
		} catch (Exception e) {
			LastException = e;
			return false;
		}
		
		

	}

	public boolean deleteDocs(String databaseName, String collectionName, HashMap<String, Object> matchingDoc,
			boolean isMultiTrue) {

		database = mongoClient.getDatabase(databaseName);
		collection = database.getCollection(collectionName);

		try {
			if (isMultiTrue) {
				collection.deleteMany(new Document(matchingDoc));
			} else {
				collection.deleteOne(new Document(matchingDoc));
			}

			return true;
		} catch (Exception e) {
			LastException = e;
			return false;
		}

	}

	public boolean updateDocs(String databaseName, String collectionName, HashMap<String, Object> matchingDoc,
			HashMap<String, Object> newDoc, boolean isMultiTrue) {

		database = mongoClient.getDatabase(databaseName);
		collection = database.getCollection(collectionName);

		try {
			if (isMultiTrue) {
				collection.updateMany(new Document(matchingDoc), new Document("$set", new Document(newDoc)));
			} else {
				collection.updateOne(new Document(matchingDoc), new Document("$set", new Document(newDoc)));
			}

			return true;
		} catch (Exception e) {
			LastException = e;
			return false;
		}

	}

	public Exception getLastException() {
		return LastException;
	}

	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
		cleanup();
	}

	public void cleanup() {
		mongoClient.close();
	}

}