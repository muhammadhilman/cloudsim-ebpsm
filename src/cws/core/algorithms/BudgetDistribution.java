package cws.core.algorithms;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.core.core.VMType;
import cws.core.core.VMTypeCostComparator;
import cws.core.core.VMTypeCostComparatorDescending;
import cws.core.dag.DAG;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.engine.Environment;

public class BudgetDistribution {

	/**
	 * Assign budgets to each task in the DAG
	 */
	
	public static Map<Task, Double> getBudgetDistributionTopDownCheapest(List<Task> order, Double dagBudget, Environment environment) {
		
		Map<Task, Double> budgets = new HashMap<Task, Double>();
		List<VMType> vmTypes = environment.getVmTypes();
		Collections.sort(vmTypes, new VMTypeCostComparator());
		Map<Task, Integer> levels = new HashMap<Task, Integer>();
    	
		for (Task t : order){
			levels.put(t, 0);
		}
		
		int numlevels = 0;
		for (Task t : order) {
			int level = 0;
			for (Task p : t.getParents()) {
				if(levels.get(p) != null) {
					int plevel = levels.get(p);
					level = Math.max(level, plevel + 1);
				}
			}
			
			levels.put(t, level);
			numlevels = Math.max(numlevels, level + 1);
		}
		
		for (Task t : order){
    		budgets.put(t, 0.0);
		}
			
    	double budget = dagBudget;
    
    	double assignedBudget = 0.0;
    	
    	for (VMType vmType : vmTypes){
    		for (int i = 0; i < numlevels; i++){
    			for (Task t : order){
    				if (levels.get(t) == i){
    					double taskRuntime = environment.getPredictedRuntime(vmType, t);
            			taskRuntime += vmType.getProvisioningDelay().sample();
            			double taskCost = environment.getCost(taskRuntime, vmType);
            			assignedBudget = (assignedBudget - budgets.get(t)) + taskCost;
            			if (assignedBudget <= budget){
            				budgets.put(t,taskCost);
            			} else {
            				assignedBudget = assignedBudget + budgets.get(t) - taskCost;
            			}
    				}   
        		}
    		}
    	}
    	    	
    	return budgets;
	}
		
	public static Map<Task, Double> getBudgetDistributionTopDownFastest(List<Task> order, Double dagBudget, Environment environment) {
		
		HashMap<Task, Double> budgets = new HashMap<Task, Double>();
		
		List<VMType> vmTypes = environment.getVmTypes();
		Collections.sort(vmTypes, new VMTypeCostComparatorDescending());
		
		HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
		
		for (Task t : order){
			levels.put(t, 0);
		}

	    int numlevels = 0;
		for (Task t : order) {
			int level = 0;
			for (Task p : t.getParents()) {
				if(levels.get(p) != null) {
					int plevel = levels.get(p);
					level = Math.max(level, plevel + 1);
				}
			}
			levels.put(t, level);
			numlevels = Math.max(numlevels, level + 1);
		}
		
		for (Task t : order){
    		budgets.put(t, 0.0);
		}
		
    	double budget = dagBudget;
    	
    	for (int i = 0; i < numlevels; i++){
    		for (Task t : order){
    			if (levels.get(t) == i){
    				for (VMType vmType : vmTypes){
            			double taskRuntime = environment.getPredictedRuntimeWithDegradation(vmType, t);
            			taskRuntime += vmType.getProvisioningDelay().sample();
            			double taskCost = environment.getCost(taskRuntime, vmType);
            			//separate new code here
            			if(budget > taskCost){
            				budgets.put(t, taskCost);
            				budget = budget - taskCost;
            				break;
            			} else {
            				budgets.put(t, budget);
            				budget = 0;
            				break;
            			}
            		}
    			}
        	}
    	}
    	    	
    	return budgets;
	}
	
	public static HashMap<Task, Double> getBudgetDistributionLevelCheapest(Double alpha, TopologicalOrder order, DAG dag,
			HashMap<Task, Double> runtimes, Environment environment) {
		
		HashMap<Task, Double> budgets = new HashMap<Task, Double>();
		
		List<VMType> vmTypes = environment.getVmTypes();
		Collections.sort(vmTypes, new VMTypeCostComparator());
		
		double dagBudget = 0.0;
    	dagBudget = dag.getBudget();
		
		for (Task t : order){
    		budgets.put(t, 0.0);
    	}
		
		// The level of each task is max[p in parents](p.level) + 1
		HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
		int numlevels = 0;
		for (Task t : order) {
			int level = 0;
			for (Task p : t.getParents()) {
				if(levels.get(p) != null) {
					int plevel = levels.get(p);
					level = Math.max(level, plevel + 1);
				}
			}
			levels.put(t, level);
			numlevels = Math.max(numlevels, level + 1);
		}
		
		/*
		 * Compute: 1. Total number of tasks in DAG 2. Total number of tasks in
		 * each level 3. Total runtime of tasks in DAG 4. Total runtime of tasks
		 * in each level
		 */
		double totalTasks = 0;
		double[] totalTasksByLevel = new double[numlevels];

		double totalRuntime = 0;
		double[] totalRuntimesByLevel = new double[numlevels];

		for (Task task : order) {
			double runtime = runtimes.get(task);
			int level = levels.get(task);

			totalRuntime += runtime;
			totalRuntimesByLevel[level] += runtime;

			totalTasks += 1;
			totalTasksByLevel[level] += 1;
		}
		
		//calculate totalRank
		int sumRank = 0;
		for(int i = 0; i < numlevels; i++){
			sumRank += i;
		}
		
		//calculate budget for each level proportional based on number of task,
		//number of runtimes, and rank level
    	double[] levelBudget = new double[numlevels];
    	double tempBudget = dagBudget;
    	
    	for(int i = 0; i < numlevels; i++){
    		double taskPart = alpha * (totalTasksByLevel[i] / totalTasks);
    		double runtimePart = (1 - alpha) * (totalRuntimesByLevel[i] / totalRuntime);
    		double rankPart = (numlevels - i) / sumRank;
    		
    		levelBudget[i] = Math.round((taskPart + runtimePart + rankPart) * dagBudget);
    		
    		if(i == numlevels - 1){
    			levelBudget[i] = tempBudget;
    		}
    		
    		tempBudget = tempBudget - levelBudget[i];
    	}
    	
    	//calculate budget for each task in each level
   		for (int i = 0; i < numlevels; i++){
   			double budgetLevel = levelBudget[i];
   	    	for (VMType vmType : vmTypes){
   	    		for (Task t : order){
   	    			if (levels.get(t) == i){
   	    				double taskRuntime = environment.getPredictedRuntime(vmType, t);
   	   	    			double taskCost = environment.getVMCostFor(vmType, taskRuntime);
   	   	    			budgetLevel = (budgetLevel + budgets.get(t)) - taskCost;
   	   	    			if (budgetLevel >= 0.0){
   	   	    				budgets.put(t,taskCost);
   	   	    			} else if (budgetLevel < 0.0){
   	   	    				budgetLevel = (budgetLevel - budgets.get(t)) + taskCost;
   	   	    			}
   	   	    			
   	    			}
   	    		}
   	    	}
   	    	if(i < numlevels-1){
   				levelBudget[i+1] = levelBudget[i+1] + budgetLevel;
   	    	}
   		}
   		
		return budgets;
	}
	
	public static HashMap<Task, Double> getBudgetDistributionLevelFastest(Double alpha, TopologicalOrder order, DAG dag,
			HashMap<Task, Double> runtimes, Environment environment) {
		
		HashMap<Task, Double> budgets = new HashMap<Task, Double>();
		
		List<VMType> vmTypes = environment.getVmTypes();
		Collections.sort(vmTypes, new VMTypeCostComparatorDescending());
		
		double dagBudget = 0.0;
    	dagBudget = dag.getBudget();
		
		for (Task t : order){
    		budgets.put(t, 0.0);
    	}
		
		// The level of each task is max[p in parents](p.level) + 1
		HashMap<Task, Integer> levels = new HashMap<Task, Integer>();
		int numlevels = 0;
		for (Task t : order) {
			int level = 0;
			for (Task p : t.getParents()) {
				if(levels.get(p) != null) {
					int plevel = levels.get(p);
					level = Math.max(level, plevel + 1);
				}
			}
			levels.put(t, level);
			numlevels = Math.max(numlevels, level + 1);
		}
		
		/*
		 * Compute: 1. Total number of tasks in DAG 2. Total number of tasks in
		 * each level 3. Total runtime of tasks in DAG 4. Total runtime of tasks
		 * in each level
		 */
		double totalTasks = 0;
		double[] totalTasksByLevel = new double[numlevels];

		double totalRuntime = 0;
		double[] totalRuntimesByLevel = new double[numlevels];

		for (Task task : order) {
			double runtime = runtimes.get(task);
			int level = levels.get(task);

			totalRuntime += runtime;
			totalRuntimesByLevel[level] += runtime;

			totalTasks += 1;
			totalTasksByLevel[level] += 1;
		}
		
		//calculate totalRank
		int sumRank = 0;
		for(int i = 0; i < numlevels; i++){
			sumRank += i;
		}
		
		//calculate budget for each level proportional based on number of task,
		//number of runtimes, and rank level
    	double[] levelBudget = new double[numlevels];
    	double tempBudget = dagBudget;
    	
    	for(int i = 0; i < numlevels; i++){
    		double taskPart = alpha * (totalTasksByLevel[i] / totalTasks);
    		double runtimePart = (1 - alpha) * (totalRuntimesByLevel[i] / totalRuntime);
    		double rankPart = (numlevels - i) / sumRank;
    		
    		levelBudget[i] = Math.round((taskPart + runtimePart + rankPart) * dagBudget);
    		
    		if(i == numlevels - 1){
    			levelBudget[i] = tempBudget;
    		}
    		
    		tempBudget = tempBudget - levelBudget[i];
    	}
    	
    	//calculate budget for each task in each level
   		for (int i = 0; i < numlevels; i++){
   			double budgetLevel = levelBudget[i];
   			for (Task t : order){
   				if (levels.get(t) == i){
   	   				for (VMType vmType : vmTypes){
   	           			double taskRuntime = environment.getPredictedRuntime(vmType, t);
   	           			double taskCost = environment.getVMCostFor(vmType, taskRuntime);
   	           			budgetLevel = budgetLevel - taskCost;
   	           			if (budgetLevel >= 0.0){
   	           				budgets.put(t,taskCost);
   	           				break;
   	           			} else if (budgetLevel < 0.0){
   	           				budgetLevel = budgetLevel + taskCost;
   	           			}
   	           		}
   	   			}
   			}
   			if(i < numlevels-1){
   				levelBudget[i+1] = levelBudget[i+1] + budgetLevel;
   			}
   		}
   		
		return budgets;
	}
	
	public static Map<Task, Double> getBudgetDistributionMSLBL(List<Task> order, Double dagBudget, Environment environment){
		Map<Task, Double> preBudgets = new HashMap<Task, Double>();
		VMType fastestVmType = environment.getFastestVM();
		VMType slowestVmType = environment.getCheapestVM();
		double maxCost = 0.0;
		double minCost = 0.0;
		
		for (Task t : order){
			double fastestRuntime = environment.getPredictedRuntimeNoTransfer(fastestVmType, t);
			//fastestRuntime += fastestVmType.getProvisioningDelay().sample();
			maxCost = maxCost + environment.getCost(fastestRuntime, fastestVmType);
			double slowestRuntime = environment.getPredictedRuntimeNoTransfer(slowestVmType, t);
			//slowestRuntime += slowestVmType.getProvisioningDelay().sample();
			minCost = minCost + environment.getCost(slowestRuntime, slowestVmType);
		}
		
		double budgetLevel = (dagBudget - minCost)/(maxCost - minCost);
		
		for (Task t : order){
			double fastestRuntime = environment.getPredictedRuntimeNoTransfer(fastestVmType, t);
			//fastestRuntime += fastestVmType.getProvisioningDelay().sample();
			double maxCostTask = environment.getCost(fastestRuntime, fastestVmType);
			double slowestRuntime = environment.getPredictedRuntimeNoTransfer(slowestVmType, t);
			//slowestRuntime += slowestVmType.getProvisioningDelay().sample();
			double minCostTask = environment.getCost(slowestRuntime, slowestVmType);
			
			double costBudgetLevel = minCostTask + ((maxCostTask - minCostTask)*budgetLevel);
			preBudgets.put(t, costBudgetLevel);
		}
		
		return preBudgets;
	}
	
}