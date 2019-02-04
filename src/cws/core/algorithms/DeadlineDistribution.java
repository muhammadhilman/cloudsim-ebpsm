package cws.core.algorithms;

import java.util.HashMap;

import org.cloudbus.cloudsim.core.CloudSim;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;

public class DeadlineDistribution {

	/**
	 * Assign deadlines proportionally to each task in the DAG based on its runtime
	 */
	public static HashMap<Task, Double> getDeadlineDistribution(TopologicalOrder order,
			HashMap<Task, Double> runtimes, double share, double startTime) {
		
		/*
		 * Compute total runtime of tasks in DAG
		 */
		double totalRuntime = 0.0;
		for (Task task : order) {
			double runtime = runtimes.get(task);
			totalRuntime += runtime;
		}

		/*
		 * The excess time share for each level is proportional to task runtime
		 * The deadline of a task t is:
		 * t.deadline = w.startTime + max[p in t.parents](p.deadline) + t.runtime + excess
		 */
		HashMap<Task, Double> deadlines = new HashMap<Task, Double>();
		for (Task task : order) {
			double excess = (runtimes.get(task) / totalRuntime) * share;
			double latestDeadline = 0.0;
			for (Task parent : task.getParents()) {
				if(deadlines.get(parent) != null) {
					double pdeadline = deadlines.get(parent);
					latestDeadline = Math.max(latestDeadline, pdeadline);
				} 
			}
			latestDeadline = Math.max(latestDeadline, CloudSim.clock());
			
			double runtime = runtimes.get(task);
			double deadline = startTime + latestDeadline + runtime + excess;
			
			if(deadline < CloudSim.clock()) {
				deadline = startTime + latestDeadline + runtime + excess;
			}
			
			deadlines.put(task, deadline);
		}

		return deadlines;
	}
}
