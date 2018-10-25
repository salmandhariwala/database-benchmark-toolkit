package querytest_local;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MongoClient client = new MongoClient();

		MongoDatabase database = client.getDatabase("test");

		MongoCollection<Document> collection = database.getCollection("mongoprod");

		
		
		AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(
				new Document("$group", new Document("_id", new Document("FundCode", "$FundCode").append("BookCode", "$BookCode"))
						.append("GrossExposure", new Document("$sum", "$GrossExposure")))));

		MongoCursor<Document> t = iterable.iterator();

		while (t.hasNext()) {
			System.out.println(t.next());
		}

		client.close();
	}

}
