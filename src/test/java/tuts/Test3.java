package tuts;

import java.io.File;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class Test3 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		final XYSeries firefox = new XYSeries("Firefox");
		
		
		
		for(int i=0;i<1000;i++){
			firefox.add(i+100, i+2000);
		}
		
		final XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(firefox);
		

		JFreeChart xylineChart = ChartFactory.createXYLineChart("Update Test", "Update Count", "Time(mili)",
				dataset, PlotOrientation.VERTICAL, true, true, false);

		int width = 640; /* Width of the image */
		int height = 480; /* Height of the image */
		File XYChart = new File("/home/salman/jfreedemo/BarChart.jpeg");
		ChartUtilities.saveChartAsJPEG(XYChart, xylineChart, width, height);
	}

}
