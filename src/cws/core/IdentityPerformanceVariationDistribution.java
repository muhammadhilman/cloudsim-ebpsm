package cws.core;

public class IdentityPerformanceVariationDistribution implements PerformanceVariationDistribution {

	@Override
	public double getPerformanceVariation() {
		//1.0 means the whole capacity.
		return 1.0;
	}

}
