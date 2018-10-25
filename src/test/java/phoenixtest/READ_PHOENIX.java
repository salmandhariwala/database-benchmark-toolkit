package phoenixtest;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import phoenix.DML;
import stopwatch.StopWatch;

public class READ_PHOENIX {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		System.out.println("going to create DML");

		DML dml = new DML(true);

		System.out.println("DML created");
		final String COMMA = ",";
		int loopcount = Integer.parseInt(args[0]);

		StopWatch watch_fetch_calc = new StopWatch(false);
		StopWatch watch_update = new StopWatch(false);

		PrintWriter p = new PrintWriter("/home/ec2-user/hbase_mongo_test/READ_PHOENIX.csv");
		p.println("UpdateCount,Time_fetch_calc(milisec),Time_update_marketvalue(milisec)");

		for (int i = 0; i < loopcount; i++) {

			watch_fetch_calc.start();
			ResultSet rset = dml.execQuery("select * from nezu_pos_2");

			HashMap<String, Integer> calc = new HashMap<String, Integer>();

			// fetching and calculation

			while (rset.next()) {
				String pk = rset.getString("pk");

				int FXRateBase = rset.getInt("FXRateBase");
				int MarketPriceBase = rset.getInt("MarketPriceBase");
				int TradeQuantity = rset.getInt("TradeQuantity");

				int marketvaluebase = FXRateBase * MarketPriceBase * TradeQuantity;

				calc.put(pk, marketvaluebase);

				System.out.println(marketvaluebase);

			}

			long time_calc_fetch = watch_fetch_calc.stop();

			watch_update.start();
			int mpcount = 0;
			for (Entry<String, Integer> entry : calc.entrySet()) {

				String key = entry.getKey();
				Integer value = entry.getValue();

				dml.update_marketvaluebase(key, value.intValue());
				System.out.println(mpcount++);
			}

			long time_update = watch_update.stop();

			p.println(i + COMMA + TimeUnit.NANOSECONDS.toMillis(time_calc_fetch) + COMMA
					+ TimeUnit.NANOSECONDS.toMillis(time_update));
			p.flush();

			System.out.println("READ[cycle] " + i);

		}

	}

}
