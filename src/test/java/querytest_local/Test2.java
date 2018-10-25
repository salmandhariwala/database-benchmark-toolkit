package querytest_local;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		MongoClient client = new MongoClient();

		MongoDatabase database = client.getDatabase("test");

		MongoCollection<Document> collection = database.getCollection("mongoprod");

		// ===================================

		ArrayList<String> groupBy = new ArrayList<String>();
		ArrayList<String> sum = new ArrayList<String>();
		
		groupBy.add("FundCode");
		groupBy.add("BookCode");
		
		sum.add("GrossExposure");

		Document group_fun = genrateQuery(groupBy, sum);

		// ==================================

		// declare a clause
		Document claues = new Document();

		// prepare your id
		Document where = new Document();

		where.append("FundCode", "$FundCode");
		where.append("BookCode", "$BookCode");

		// add your id to the clause
		claues.append("_id", where);

		// following add your
		claues.append("GrossExposure", new Document("$sum", "$GrossExposure"));

		// declare group
		Document group = new Document("$group", claues);

		// pass group for aggregation

		AggregateIterable<Document> iterable = collection.aggregate(Arrays.asList(group_fun));

		MongoCursor<Document> t = iterable.iterator();

		while (t.hasNext()) {
			System.out.println(t.next());
		}

		client.close();

	}

	private static Document genrateQuery(ArrayList<String> groupBy, ArrayList<String> sum) {
		// TODO Auto-generated method stub

		Document claues = new Document();

		Document where = new Document();

		for (String temp : groupBy) {
			where.append(temp, "$" + temp);
		}
		
		claues.append("_id", where);
		
		for (String temp : sum) {
			claues.append(temp, new Document("$sum", "$"+temp));
		}

		Document group = new Document("$group", claues);
		
		return group;
	}

	private static String appendDollar(String string) {
		// TODO Auto-generated method stub

		return "$" + string;
	}

}
