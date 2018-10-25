package mongotest;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import mongo.MongoCRUD;
import mongo.MongoFactory;
import random.RanGen;
import stopwatch.StopWatch;

public class AdhocTest_FX {

	public static void main(String[] args) throws Exception {

		//printwriter
		PrintWriter p=new PrintWriter("/home/ec2-user/hbase_mongo_test/result_text/FX.csv");
		
		// number of update
		int num_of_update = Integer.parseInt(args[0]);

		// declaring stopwatch
		StopWatch watch = new StopWatch(true, "/home/ec2-user/hbase_mongo_test/result_text/FX.txt");

		// declare mongocrud
		MongoCRUD crud = new MongoCRUD("10.0.1.90", 10001);

		// declaring random genrator
		RanGen random = new RanGen(1, 10000);

		// declaring mongoFactory
		MongoFactory factory = new MongoFactory("10.0.1.90", 10001);

		// updating each doc n times

		for (int i = 0; i < num_of_update; i++) {

			watch.start();

			HashMap<String, Object> match = new HashMap<String, Object>();

			match.put("LocalCurrencyCode", factory.getRandomFx());

			ArrayList<HashMap<String, Object>> t = crud.getDocs("nezu_data_db", "nezu_position_denorm_data_coll_new",
					match);

			// doing expensive operations

			int currentFx = random.get();

			for (HashMap<String, Object> temp : t) {

				HashMap<String, Object> temp1 = new HashMap<String, Object>(temp);

				temp1.put("FXRateBase", currentFx);

				crud.updateDocs("nezu_data_db", "nezu_position_denorm_data_coll_new", temp, temp1, false);

			}

			long duration=watch.stop();
			
			p.println(i+","+t.size()+","+TimeUnit.NANOSECONDS.toMillis(duration));
			p.flush();

		}

		// making Chart

		watch.genrateChart("test", "/home/ec2-user/hbase_mongo_test/result_visual/FX.jpeg", 640, 480, "FX Change",
				"Update_Count", "Time (Nano sec)");

	}

}
