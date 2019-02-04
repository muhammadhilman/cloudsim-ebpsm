package cws.core.core;

import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

public class VMType implements Cloneable {
    /**
     * The processing power of this VM
     */
    private final int mips;

    /**
     * The number of cores of this VM
     */
    private final int cores;

    /**
     * Price per billing unit of usage
     */
    private final double billingUnitPrice;

    /**
     * For how long we pay in advance
     */
    private final double billingTimeInSeconds;

    /**
     * Delay from when the VM is launched until it is ready
     */
    private final ContinuousDistribution provisioningDelay;

    /**
     * Delay from when the VM is terminated until it is no longer charged
     */
    private final ContinuousDistribution deprovisioningDelay;

    /**
     * The number of bytes on internal disk that can be used as a cache
     * @see {@link cws.core.storage.cache.VMCacheManager}
     */
    private final long cacheSize;

    /**
     * The amount of memory available
     */
    private final double memory;
    
    /**
     * Unique name of the instance
     */
    private final String name;
    
    public int getMips() {
        return mips;
    }

    public int getCores() {
        return cores;
    }

    public double getPriceForBillingUnit() {
        return billingUnitPrice;
    }

    public double getBillingTimeInSeconds() {
        return billingTimeInSeconds;
    }

    public ContinuousDistribution getProvisioningDelay() {
        return provisioningDelay;
    }

    public ContinuousDistribution getDeprovisioningDelay() {
        return deprovisioningDelay;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public double getMemory() {
    	return memory;
    }

    public String getName() {
    	return name;
    }
    
    public VMType(int mips, int cores, double billingUnitPrice, double billingTimeInSeconds,
            ContinuousDistribution provisioningTime, ContinuousDistribution deprovisioningTime, long cacheSize,
            double memory, String name) {
        this.mips = mips;
        this.cores = cores;
        this.billingUnitPrice = billingUnitPrice;
        this.billingTimeInSeconds = billingTimeInSeconds;
        this.provisioningDelay = provisioningTime;
        this.deprovisioningDelay = deprovisioningTime;
        this.cacheSize = cacheSize;
        this.memory = memory;
        this.name = name;
    }
}
