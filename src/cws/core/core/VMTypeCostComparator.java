package cws.core.core;

import java.util.Comparator;

import cws.core.core.VMType;

/**
 * Sorts a list of VMTypes in ascending order based on Cost/Hour
 * @author hilman
 *
 */

public class VMTypeCostComparator implements Comparator<VMType> {
	
	@Override
	public int compare(VMType o1, VMType o2) {
		if(o1.getPriceForBillingUnit() > o2.getPriceForBillingUnit()) {
			return 1;
		} else if (o1.getPriceForBillingUnit() < o2.getPriceForBillingUnit()) {
			return -1;
		}
		return 0;
	}
}
