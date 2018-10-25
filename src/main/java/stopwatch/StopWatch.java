package stopwatch;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import render.Renderer;

public class StopWatch {

	private ArrayList<Long> durations = new ArrayList<Long>();

	private long startTime;
	private long stopTime;

	private boolean isFileWriteEnabled;
	private PrintWriter p;

	public StopWatch(boolean isFileWriteEnabled, String fileName) throws FileNotFoundException {

		this.isFileWriteEnabled = isFileWriteEnabled;
		p = new PrintWriter(fileName);
	}

	public StopWatch(boolean isFileWriteEnabled) {

		this.isFileWriteEnabled = isFileWriteEnabled;

	}

	public void start() {
		startTime = System.nanoTime();
	}

	public long stop() {
		stopTime = System.nanoTime();

		long stopTime_dur = stopTime - startTime;

		durations.add(stopTime_dur);

		if (isFileWriteEnabled) {
			p.println(stopTime_dur);
			p.flush();
		}

		return stopTime_dur;
	}

	public void genrateChart(String seriesName, String fileName, int width, int height, String Heading, String X_axis,
			String Y_axis) {

		Renderer render = new Renderer();

		double[] d = new double[durations.size()];

		for (int i = 0; i < durations.size(); i++) {
			d[i] = durations.get(i);
		}

		render.setSeries(seriesName, d);

		render.render(fileName, width, height, Heading, X_axis, Y_axis);
	}

}
