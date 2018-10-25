package test;

import render.Renderer;

public class Test1 {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		Renderer render = new Renderer();

		double[] d = { 10, 20, 40, 0, 23, 10, 20, 40, 0, 23, 10, 20, 40, 0, 23 };
		render.setSeries("test1", d);

		double[] d1 = { 110, 120, 140, 10, 123, 10, 20, 40, 0, 23, 10, 20, 40, 0, 23, 10, 20, 40, 0, 23 };
		render.setSeries("test2", d1);

		render.render("/home/salman/jfreedemo/test.jpeg", 640, 480, "Test", "x-axis", "Y-axis");

	}

}
