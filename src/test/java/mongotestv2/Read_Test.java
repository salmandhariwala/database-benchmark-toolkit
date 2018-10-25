package mongotestv2;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;

import stopwatch.StopWatch;

public class Read_Test {

	public static void main(String[] args) throws Exception {

		// processing args[]
		String filePath = "/home/ec2-user/hbase_mongo_test/READ.csv";
		int loop_count = Integer.parseInt(args[0]);

		// Mongo Stuffs
		MongoClient client = new MongoClient("10.0.1.90", 10001);
		MongoDatabase database = client.getDatabase("nezu_data_db");
		MongoCollection<Document> collection = database.getCollection("nezu_position_denorm_data_coll_new");

		// bulk update

		// Stopwatch
		StopWatch watch_fetch = new StopWatch(false);
		StopWatch watch_process = new StopWatch(false);
		StopWatch watch_update = new StopWatch(false);

		// printwriter stuffs
		PrintWriter p = new PrintWriter(filePath);
		p.println("loop count,fetch_time,process_time,update_time");
		p.flush();

		for (int i = 0; i < loop_count; i++) {

			long fetch_time, process_time, update_time;

			watch_fetch.start();

			// projection

			Bson project = new Document("_id", true).append("FXRateBase", true).append("MarketPriceBase", true)
					.append("TradeQuantity", true);

			// fetched all the documents
			MongoCursor<Document> cursor = collection.find(new Document()).projection(project).iterator();

			// declaring in Memory data str
			ArrayList<Document> allDocument = new ArrayList<Document>();

			// looping and adding in memory

			while (cursor.hasNext()) {

				allDocument.add(cursor.next());

			}

			fetch_time = watch_fetch.stop();

			// fetching done

			watch_process.start();

			// starting processing

			HashMap<Object, Double> calculated = new HashMap<Object, Double>();

			for (Document temp : allDocument) {

				double FXRateBase;
				double MarketPriceBase;
				double TradeQuantity;

				try {
					FXRateBase = temp.getInteger("FXRateBase");
				} catch (Exception e) {
					FXRateBase = temp.getDouble("FXRateBase");
				}

				try {
					MarketPriceBase = temp.getInteger("MarketPriceBase");
				} catch (Exception e) {
					MarketPriceBase = temp.getDouble("MarketPriceBase");
				}

				try {
					TradeQuantity = temp.getInteger("TradeQuantity");
				} catch (Exception e) {
					TradeQuantity = temp.getDouble("TradeQuantity");
				}

				double marketvaluebase = FXRateBase * MarketPriceBase * TradeQuantity;

				Object _id = temp.get("_id");

				calculated.put(_id, marketvaluebase);

			}

			process_time = watch_process.stop();
			// done with processing now have mapping for id and market value

			// going for update

			watch_update.start();

			ArrayList<UpdateOneModel<Document>> listModal = new ArrayList<UpdateOneModel<Document>>();

			for (Entry<Object, Double> entry : calculated.entrySet()) {

				Object key = entry.getKey();
				Double value = entry.getValue();

				// collection.updateOne(new Document("_id", key),
				// new Document("$set", new Document("marketvaluebase",
				// value)));

				listModal.add(new UpdateOneModel<Document>(new Document("_id", key),
						new Document("$set", new Document("marketvaluebase", value))));
				//
				// if (batchCounter == 20000) {
				// collection.bulkWrite(listModal);
				// listModal.clear();
				// batchCounter = 0;
				//
				// }

			}

			collection.bulkWrite(listModal);

			update_time = watch_update.stop();
			// done with updation

			// writing down to file
			p.println(i + "," + TimeUnit.NANOSECONDS.toMillis(fetch_time) + ","
					+ TimeUnit.NANOSECONDS.toMillis(process_time) + "," + TimeUnit.NANOSECONDS.toMillis(update_time));
			p.flush();

			System.out.println("updated cycle Count [READ]:" + i);

		}

		client.close();
		p.close();
	}

}
