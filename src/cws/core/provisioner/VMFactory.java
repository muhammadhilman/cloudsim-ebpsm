package cws.core.provisioner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cws.core.FailureModel;
import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.RuntimeDistribution;
import cws.core.jobs.UniformRuntimeDistribution;
import cws.core.NormalPerformanceVariationDistribution;
import cws.core.IdentityPerformanceVariationDistribution;
import cws.core.PerformanceVariationDistribution;

public class VMFactory {
    private static final double DEFAULT_RUNTIME_VARIANCE = 0.0;
    private static final double DEFAULT_FAILURE_RATE = 0.0;
    private static final double DEFAULT_AVG_PERFORMANCE_VAR = 0.0;
    private static final double DEFAULT_MAX_PERFORMANCE_VAR = 0.0;
    private static final double DEFAULT_STDDEV_PERFORMANCE_VAR = 0.0;

    private static RuntimeDistribution runtimeDistribution = new IdentityRuntimeDistribution();
    private static PerformanceVariationDistribution pvDistribution = new IdentityPerformanceVariationDistribution();
    private static FailureModel failureModel = new FailureModel(0, 0.0);
    
    
    private static double runtimeVariance;
    private static double failureRate;
    private static double avgPerformanceVar;
    private static double maxPerformanceVar;
    private static double stddevPerformanceVar;

    public static void setRuntimeDistribution(RuntimeDistribution runtimeDistribution) {
        VMFactory.runtimeDistribution = runtimeDistribution;
    }

    public static RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public static FailureModel getFailureModel() {
        return failureModel;
    }

    public static void setFailureModel(FailureModel failureModel) {
        VMFactory.failureModel = failureModel;
    }

    public static void setPerformanceVariationDistribution(PerformanceVariationDistribution pvDistribution) {
        VMFactory.pvDistribution = pvDistribution;
    }
    
    public static PerformanceVariationDistribution getPerformanceVariationDistribution() {
        return pvDistribution;
    }
    
    /**
     * @param cloudSimWrapper - initialized CloudSimWrapper instance. It needs to be inited, because we're creting
     *            storage manager here.
     */
    public static VM createVM(VMType vmType, CloudSimWrapper cloudSimWrapper) {
        VM vm = new VM(vmType, cloudSimWrapper);
        vm.setRuntimeDistribution(runtimeDistribution);
        vm.setFailureModel(failureModel);
        vm.setPvDistribution(pvDistribution);
        return vm;
    }

    public static void buildCliOptions(Options options) {
        Option runtimeVariance = new Option("rv", "runtime-variance", true, "Runtime variance, defaults to "
                + DEFAULT_RUNTIME_VARIANCE);
        runtimeVariance.setArgName("VAR");
        options.addOption(runtimeVariance);

        Option failureRate = new Option("fr", "failure-rate", true, "Faliure rate, defaults to " + DEFAULT_FAILURE_RATE);
        failureRate.setArgName("RATE");
        options.addOption(failureRate);
        
        Option avgPerformanceVar = new Option("avgpv", "average-performance-variation", true, "Average performance variation, defaults to "
                + DEFAULT_AVG_PERFORMANCE_VAR);
        runtimeVariance.setArgName("AVGPV");
        options.addOption(avgPerformanceVar);
        
        Option MaxPerformanceVar = new Option("maxgpv", "max-performance-variation", true, "Maximum performance variation, defaults to "
                + DEFAULT_MAX_PERFORMANCE_VAR);
        runtimeVariance.setArgName("MAXGPV");
        options.addOption(MaxPerformanceVar);
        
        Option stddevPerformanceVar = new Option("stddevgpv", "stddev-performance-variation", true, "Standard deviation of the performance variation, defaults to "
                + DEFAULT_STDDEV_PERFORMANCE_VAR);
        runtimeVariance.setArgName("STDDEVGPV");
        options.addOption(stddevPerformanceVar);
    }

    public static void readCliOptions(CommandLine args, long seed) {
    	runtimeVariance = Double.parseDouble(args.getOptionValue("runtime-variance", DEFAULT_RUNTIME_VARIANCE + ""));
        failureRate = Double.parseDouble(args.getOptionValue("failure-rate", DEFAULT_FAILURE_RATE + ""));
        
        avgPerformanceVar = Double.parseDouble(args.getOptionValue("average-performance-variation", DEFAULT_AVG_PERFORMANCE_VAR + ""));
        maxPerformanceVar = Double.parseDouble(args.getOptionValue("max-performance-variation", DEFAULT_MAX_PERFORMANCE_VAR + ""));
        stddevPerformanceVar = Double.parseDouble(args.getOptionValue("stddev-performance-variation", DEFAULT_STDDEV_PERFORMANCE_VAR + ""));
        
        System.out.printf("runtimeVariance = %f\n", runtimeVariance);
        System.out.printf("failureRate = %f\n", failureRate);
        System.out.printf("performanceVariation = %f avg, %f max, %f stddev\n", avgPerformanceVar, maxPerformanceVar, stddevPerformanceVar);

        if (runtimeVariance > 0.0) {
            VMFactory.setRuntimeDistribution(new UniformRuntimeDistribution(seed, runtimeVariance));
        }

        if (failureRate > 0.0) {
            VMFactory.setFailureModel(new FailureModel(seed, failureRate));
        }
        
        if(avgPerformanceVar > 0.0) {
        	VMFactory.setPerformanceVariationDistribution(new NormalPerformanceVariationDistribution(seed, avgPerformanceVar, stddevPerformanceVar, maxPerformanceVar));
        }
    }

    public static double getRuntimeVariance() {
        return runtimeVariance;
    }

    public static double getFailureRate() {
        return failureRate;
    }
}
