package mongotestv2;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

import mongo.MongoFactory;
import random.RanGen;
import stopwatch.StopWatch;

public class Trade_test {

	public static void main(String[] args) throws Exception {

		// printwriter
		PrintWriter p = new PrintWriter("/home/ec2-user/hbase_mongo_test/TRADE.csv");
		p.println("UpdateCount,UpdatedDocument,Time(milisec)");

		// number of update
		int num_of_update = Integer.parseInt(args[0]);

		MongoFactory factory = new MongoFactory("10.0.1.90", 10001);

		RanGen random = new RanGen(1, 1000);

		// declaring stopwatch
		StopWatch watch = new StopWatch(false);

		MongoClient mongoClient = new MongoClient("10.0.1.90", 10001);

		MongoDatabase database = mongoClient.getDatabase("nezu_data_db");

		MongoCollection<Document> collection = database.getCollection("nezu_position_denorm_data_coll_new");

		for (int i = 0; i < num_of_update; i++) {

			watch.start();

			UpdateResult t = collection.updateMany(new Document("PositionKey", new Double(factory.getRandomPosition())),
					new Document("$set", new Document("TradeQuantity", random.get())));

			long duration = watch.stop();

			p.println(i + "," + t.getModifiedCount() + "," + TimeUnit.NANOSECONDS.toMillis(duration));
			p.flush();
			
			System.out.println("cycle Count [TRADE]:"+i);
		}

		mongoClient.close();
		p.close();
	}

}
