package test;

import java.util.concurrent.ThreadLocalRandom;

import stopwatch.StopWatch;

public class Test2 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		StopWatch watch = new StopWatch(true, "/home/salman/jfreedemo/test2.txt");

		for (int i = 0; i < 1000; i++) {
			watch.start();

			
			
			Thread.sleep(500+ThreadLocalRandom.current().nextInt(100, 500 + 1));

			watch.stop();
		}
		
		watch.genrateChart("test-series", "/home/salman/jfreedemo/test2.jpeg", 640, 480, "TestHeading", "Update_Count", "Time_nane");

	}

}
