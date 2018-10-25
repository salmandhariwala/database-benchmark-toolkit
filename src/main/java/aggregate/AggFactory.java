package aggregate;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import mongo.QueryFactory;

public class AggFactory {

	MongoClient client;
	MongoDatabase db;
	MongoCollection<Document> collection;

	QueryFactory queryFactory = new QueryFactory();

	public AggFactory(boolean isProd) {

		if (!isProd) {
			client = new MongoClient();
			db = client.getDatabase("sampledb");
			collection = db.getCollection("samplecoll");
		} else {
			client = new MongoClient("10.0.1.90", 10001);
			db = client.getDatabase("nezu_data_db");
			collection = db.getCollection("nezu_position_denorm_data_coll_new");
		}

	}

	public Document execAggQuery() {

		MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(queryFactory.genrateQuery())).iterator();

		return cursor.next();

	}

	public void close() {
		client.close();

	}

	public String getGroupby() {
		return queryFactory.getGroupby();
	}

}
