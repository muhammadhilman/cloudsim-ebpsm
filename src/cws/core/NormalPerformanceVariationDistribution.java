package cws.core;

import java.util.Random;

public class NormalPerformanceVariationDistribution implements PerformanceVariationDistribution {

	private Random random;
	private double average;
	private double stddev;
	private double maxVar;
	
	public NormalPerformanceVariationDistribution(long seed, double average, double stddev, double maxVar) {
		this.random = new Random(seed);
		this.average = average;
		this.stddev = stddev;
		this.maxVar = maxVar;
	}
	
	@Override
	/**Performance loss
	 * is modelled after ''Performance Analysis of High Performance Computing Applications on the
	 * Amazon Web Services Cloud'' by Jackson et al: */
	public double getPerformanceVariation() {
		/*Jackson experienced up to 30% variability.
		 * So, we will use 15% in average, 10% stddev
		 */
		double performanceLoss = (random.nextGaussian()*stddev) + average;
		if(performanceLoss > maxVar) performanceLoss = maxVar;
		else if (performanceLoss < 0.0) performanceLoss = 0.0;
		
		return 1.0 - performanceLoss;
	}

}
