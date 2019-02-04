package cws.core.engine;

import java.util.Random;

import cws.core.core.VMType;

import cws.core.dag.Task;
import cws.core.storage.StorageManager;

/**
 * Storage aware prediction strategy. It takes file transfers into account.
 */
public class StorageAwarePredictionStrategy implements PredictionStrategy {

    /**
     * Storage aware version of runtime prediction. It takes file transfers into account.
     * 
     * @see ({@link #getPredictedRuntime(Task, VMType, StorageManager)}
     */
    @Override
    public double getPredictedRuntime(Task task, VMType vmType, StorageManager storageManager) {
        return task.getSize() / vmType.getMips() + storageManager.getTransferTimeEstimation(task);
    }
    
    @Override
	public double getPredictedRuntimeNoTransfer(Task task, VMType vmType) {
    	return task.getSize() / vmType.getMips();
	}
    
    @Override
	public double getPredictedRuntimeWithDegradation(Task task,
			VMType vmType, StorageManager storageManager) {
		double var = getPerformanceVariation();
		return (task.getSize() /(vmType.getMips()*var)) + storageManager.getTransferTimeEstimationWithDegradation(task);
	}
	
	/**Performance loss
	 * is modelled after ''Performance Analysis of High Performance Computing Applications on the
	 * Amazon Web Services Cloud'' by Jackson et al: */
	public double getPerformanceVariation() {
		/*Jackson experienced up to 30% variability.
		 * So, we will use 15% in average, 10% stddev
		 */
		Random random = new Random();
		double performanceLoss = (random.nextGaussian()*0.10) + 0.15;
		if(performanceLoss > 0.30) performanceLoss = 0.30;
		else if (performanceLoss < 0.0) performanceLoss = 0.0;
		
		return 1.0 - performanceLoss;
	}
    
}
