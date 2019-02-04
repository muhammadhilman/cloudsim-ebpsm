package cws.core.core;

import java.util.Comparator;

import cws.core.core.VMType;

/**
 * Sorts a list of VMTypes in ascending order based on MIPS
 * @author hilman
 *
 */

public class VMTypeMipsComparator implements Comparator<VMType> {
	
	@Override
	public int compare(VMType o1, VMType o2) {
		if(o1.getMips() > o2.getMips()) {
			return 1;
		} else if (o1.getMips() < o2.getMips()) {
			return -1;
		}
		return 0;
	}
}
