package aggregate;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import stopwatch.StopWatch;

public class AggThread extends Thread {

	AggFactory factory;

	PrintWriter p;

	int loop_count;
	String name;

	public final String COMMA = ",";

	stopwatch.StopWatch watch = new StopWatch(false);

	public AggThread(PrintWriter p, int loop_count, String name, Boolean isprod) {

		factory = new AggFactory(isprod);

		this.p = p;
		this.loop_count = loop_count;
		this.name = name;

	}

	public void run() {

		for (int i = 0; i < loop_count; i++) {

			watch.start();
			factory.execAggQuery();
			long time = watch.stop();
			p.println(name + COMMA + factory.getGroupby() + COMMA + TimeUnit.NANOSECONDS.toMillis(time));
			p.flush();
		}

		factory.close();

	}

}
