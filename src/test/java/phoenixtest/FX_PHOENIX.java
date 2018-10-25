package phoenixtest;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import mongo.MongoFactory;
import phoenix.DML;
import random.RanGen;
import stopwatch.StopWatch;

public class FX_PHOENIX {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		final String COMMA = ",";
		int loopcount = Integer.parseInt(args[0]);

		MongoFactory factory = new MongoFactory("10.0.1.90", 10001);

		DML dml = new DML(true);
		RanGen random = new RanGen(1, 1000);

		StopWatch keyfetch = new StopWatch(false);
		StopWatch keyupdate = new StopWatch(false);

		PrintWriter p = new PrintWriter("/home/ec2-user/hbase_mongo_test/FX_PHOENIX.csv");
		p.println("UpdateCount,UpdatedDocument,Time_fetch_key(milisec),Time_update_key(milisec)");

		for (int i = 0; i < loopcount; i++) {

			String LocalCurrencyCode = factory.getRandomFx();
			int value = random.get();

			//***********************//
			// fetching keys
			keyfetch.start();
			ArrayList<String> pk = dml.getkeys_LocalCurrencyCode(LocalCurrencyCode);
			long keyfetchtime = keyfetch.stop();

			// updating keys
			keyupdate.start();
			for (String temp : pk) {
				dml.update_FXRateBase(temp, value);
			}
			long keyupdatetime = keyupdate.stop();

			//***********************//
			
			p.println(i + COMMA + pk.size() + COMMA + TimeUnit.NANOSECONDS.toMillis(keyfetchtime) + COMMA
					+ TimeUnit.NANOSECONDS.toMillis(keyupdatetime));
			p.flush();

			System.out.println("Fx[cycle] " + i);

		}

		p.close();
		dml.close();

	}

}
