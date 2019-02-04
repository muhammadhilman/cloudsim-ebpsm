package cws.core;

public interface PerformanceVariationDistribution {

	/* 
	 * Returns how much % of the capacity is being actually delivered to the VM.
	 * 1.0 means the whole capacity.
	 */
	public double getPerformanceVariation();
}
