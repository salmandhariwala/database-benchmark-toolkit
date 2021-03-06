package phoenixtest;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import mongo.MongoFactory;
import phoenix.DML;
import random.RanGen;
import stopwatch.StopWatch;

public class TRADE_PHOENIX {
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		final String COMMA = ",";
		int loopcount = Integer.parseInt(args[0]);

		MongoFactory factory = new MongoFactory("10.0.1.90", 10001);

		DML dml = new DML(true);
		RanGen random = new RanGen(1, 1000);

		StopWatch keyfetch = new StopWatch(false);
		StopWatch keyupdate = new StopWatch(false);

		PrintWriter p = new PrintWriter("/home/ec2-user/hbase_mongo_test/TRADE_PHOENIX.csv");
		p.println("UpdateCount,UpdatedDocument,Time_fetch_key(milisec),Time_update_key(milisec)");

		for (int i = 0; i < loopcount; i++) {

			String position = factory.getRandomPosition();
			int value = random.get();

			//***********************//
			// fetching keys
			keyfetch.start();
			ArrayList<String> pk = dml.getkeys_PositionKey(position);
			long keyfetchtime = keyfetch.stop();

			// updating keys
			keyupdate.start();
			for (String temp : pk) {
				dml.update_TradeQuantity(temp, value);
			}
			long keyupdatetime = keyupdate.stop();

			//***********************//
			
			p.println(i + COMMA + pk.size() + COMMA + TimeUnit.NANOSECONDS.toMillis(keyfetchtime) + COMMA
					+ TimeUnit.NANOSECONDS.toMillis(keyupdatetime));
			p.flush();

			System.out.println("TRADE[cycle] " + i);

		}

		p.close();
		dml.close();

	}
}
