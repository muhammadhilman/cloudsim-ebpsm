package cws.core.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import cws.core.VM;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGFile;
import cws.core.dag.Task;
import cws.core.core.VMTypeMipsComparator;
import cws.core.core.VMTypeCostComparator;
import cws.core.storage.StorageManager;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.cache.VMCacheManager;

public class Environment {
    private VMType vmType;
    private List<VMType> vmTypes;
    private StorageManager storageManager;

    /**
     * The prediction strategy, i.e. how we will predict task's runtime.
     */
    private PredictionStrategy predictionStrategy;

    public Environment(List<VMType> vmTypes, StorageManager storageManager, PredictionStrategy predictionStrategy) {//, VM2VMTransferManager transferManager) {
        this.vmTypes = vmTypes;
        this.storageManager = storageManager;
        this.predictionStrategy = predictionStrategy;
        this.vmType = vmTypes.get(0);
    }

    // FIXME(mequrel): temporary encapsulation breakage for static algorithm, dynamic algorithm and provisioners
    public VMType getVMType() {
        return vmType;
    }

    /**
     * Returns task's predicted runtime. It is based on vmType and storage manager. <br>
     * Note that the estimation is trivial and may not be accurate during congestion and it doesn't include runtime
     * variance.
     * 
     * @return task's predicted runtime as a double
     */
    public double getPredictedRuntime(Task task) {
        return predictionStrategy.getPredictedRuntime(task, vmType, storageManager);
    }
    
    public double getPredictedRuntime(VMType vmType, Task task) {
        return predictionStrategy.getPredictedRuntime(task, vmType, storageManager);
    }
    
    public double getPredictedRuntimeNoTransfer(VMType vmType, Task task){
    	return predictionStrategy.getPredictedRuntimeNoTransfer(task, vmType);
    }
    
    public double getPredictedRuntimeWithDegradation(VMType vmType, Task task) {
        return predictionStrategy.getPredictedRuntimeWithDegradation(task, vmType, storageManager);
    }
    
    public double getPredictedRuntimeOnVM(VM vm, Task task) {
    	vmType = vm.getVmType();
		return predictionStrategy.getPredictedRuntime(task, vmType, storageManager);
	}
    
    public HashMap<Task, Double> getTaskRuntimesOnSlowestVM(DAG dag){
    	HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
    	double runtime = 0.0;
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		runtime = getPredictedRuntime(getCheapestVM(), t);
    		runtimes.put(t, runtime);
    	}
    	return runtimes;
    }
    
    public HashMap<Task, Double> getTaskRuntimesOnFastestVM(DAG dag){
    	HashMap<Task, Double> runtimes = new HashMap<Task, Double>();
    	double runtime = 0.0;
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		runtime = getPredictedRuntime(getFastestVM(), t);
    		runtimes.put(t, runtime);
    	}
    	return runtimes;
    }
    
    public double getPredictedRuntime(DAG dag, VMType vmType) {
        double sum = 0.0;
        for (String taskName : dag.getTasks()) {
            sum += getPredictedRuntime(vmType, dag.getTaskById(taskName));
        }
        return sum;
    }

    public StorageManagerStatistics getStorageManagerStatistics() {
        return storageManager.getStorageManagerStatistics();
    }

    public double getCost(double runtime, VMType vmType) {
    	double billingUnits = runtime / vmType.getBillingTimeInSeconds();
    	int fullBillingUnits = (int) Math.ceil(billingUnits);
    	return Math.max(1, fullBillingUnits) * vmType.getPriceForBillingUnit();
	}
    
    public double getVMCostFor(double runtimeInSeconds) {
        double billingUnits = runtimeInSeconds / getVMType().getBillingTimeInSeconds();
        int fullBillingUnits = (int) Math.ceil(billingUnits);
        return Math.max(1, fullBillingUnits) * vmType.getPriceForBillingUnit();
    }
    
    public double getVMCostFor(VMType vmType, double runtimeInSeconds) {
        double billingUnits = runtimeInSeconds / getVMType().getBillingTimeInSeconds();
        int fullBillingUnits = (int) Math.ceil(billingUnits);
        return Math.max(1, fullBillingUnits) * vmType.getPriceForBillingUnit();
    }

    public double getSingleVMPrice() {
        return vmType.getPriceForBillingUnit();
    }

    public double getBillingTimeInSeconds() {
        return vmType.getBillingTimeInSeconds();
    }

    public PredictionStrategy getPredictionStrategy() {
        return predictionStrategy;
    }

    public double getVMProvisioningOverallDelayEstimation() {
        return vmType.getProvisioningDelay().sample() + vmType.getDeprovisioningDelay().sample();
    }

    public double getProvisioningDelayEstimation() {
        return vmType.getProvisioningDelay().sample();
    }
    
    public double getDeprovisioningDelayEstimation() {
        return vmType.getDeprovisioningDelay().sample();
    }
    
    public double getVMProvisioningOverallDelayEstimation(VMType vmType) {
        return vmType.getProvisioningDelay().sample() + vmType.getDeprovisioningDelay().sample();
    }
    
    public double getVMProvisioningDelayEstimation(VMType vmType) {
        return vmType.getProvisioningDelay().sample();
    }
    
    public double getVMDeprovisioningDelayEstimation(VMType vmType) {
        return vmType.getDeprovisioningDelay().sample();
    }
    
    public List<VMType> getVmTypes() {
    	return vmTypes;
    }

	public void setVmType(VMType vmType) {
		this.vmType = vmType;
	}
	
	//get the fastest VM Type
	public VMType getFastestVM(){
		List<VMType> vmOrderedByMips = vmTypes;
		Collections.sort(vmOrderedByMips, new VMTypeMipsComparator());
		return vmOrderedByMips.get(vmOrderedByMips.size()-1);
	}
	
	//get the cheapest VM Type
	public VMType getCheapestVM(){
		List<VMType> vmOrderedByCost = vmTypes;
		Collections.sort(vmOrderedByCost, new VMTypeCostComparator());
		return vmOrderedByCost.get(0);
	}

	public boolean isFileCached(VM vm, DAGFile file) {
		VMCacheManager cacheManager = storageManager.getCacheManager();
		return cacheManager.getFileFromCache(file, vm);
	}
}
