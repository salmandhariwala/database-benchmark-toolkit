package aggregatetest;

import java.io.PrintWriter;

import aggregate.AggThread;

public class ThreadSpawner {

	public static void main(String args[]) throws Exception {

		Boolean isprod = true;

		String fileName;
		int ThreadCount;
		int RunningCount;

		if (!isprod) {
			fileName = "/home/salman/Desktop/thread1.txt";
			ThreadCount = 20;
			RunningCount=1;
		} else {
			fileName = args[0];
			ThreadCount = Integer.parseInt(args[1]);
			RunningCount=Integer.parseInt(args[2]);
		}

		PrintWriter p = new PrintWriter(fileName);

		p.println("ThreadName,GroupBy,Time");
		p.flush();

		for (int i = 0; i < ThreadCount; i++) {
			new AggThread(p, RunningCount, "Thread-" + i, isprod).start();
		}

	}
}
