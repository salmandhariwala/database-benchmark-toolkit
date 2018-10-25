package random;

import java.util.concurrent.ThreadLocalRandom;

public class RanGen {

	int min;
	int max;

	public RanGen(int min, int max) {
		super();
		this.min = min;
		this.max = max;
	}

	public int get() {

		return ThreadLocalRandom.current().nextInt(min, max + 1);
	}

}
