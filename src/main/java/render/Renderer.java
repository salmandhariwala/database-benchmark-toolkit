package render;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class Renderer {

	XYSeriesCollection dataset = new XYSeriesCollection();

	ArrayList<XYSeries> series = new ArrayList<XYSeries>();

	public void setSeries(String seriesName, double[] datapoints) {

		XYSeries temp = new XYSeries(seriesName);

		for (int i = 0; i < datapoints.length; i++) {
			temp.add(i, datapoints[i]);
		}

		series.add(temp);

	}

	public void render(String fileName, int width, int height, String Heading, String X_axis, String Y_axis) {

		try {

			for (XYSeries temp : series) {
				dataset.addSeries(temp);
			}

			JFreeChart xylineChart = ChartFactory.createXYLineChart(Heading, X_axis, Y_axis, dataset,
					PlotOrientation.VERTICAL, true, true, false);

			ChartUtilities.saveChartAsJPEG(new File(fileName), xylineChart, width, height);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
