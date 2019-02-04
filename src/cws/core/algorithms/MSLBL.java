package cws.core.algorithms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.cloudbus.cloudsim.core.CloudSim;

import cws.core.VM;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeCostComparator;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.dag.algorithms.TopologicalOrder;
import cws.core.dag.algorithms.TopologicalOrderReverse;
import cws.core.jobs.Job;
import cws.core.provisioner.GeneralPurposeProvisioner;
import cws.core.scheduler.CloudScheduler;

public class MSLBL extends CombinedDynamicAlgorithm {
	
	private int dagFinished = 0;
	
	/** Schedule of tasks for each VM */
	private Map<VM, Job> vmPendingJob;
	
	/** Tracking budget distribution for each DAG **/
	private Map<DAG, Map<Task, Double>> dagUnscheduledBudget;
	
	/** Tracking budget distribution for each DAG **/
	private Map<DAG, Map<Task, Double>> dagBudget;
	
	/** Tracking the unscheduled task**/
	private Map<DAG, List<Task>> unscheduledTasks;
	
	/** Tracking the unfinished task**/
	private Map<DAG, List<Task>> unfinishedTasks;
		
	/** Job Execution Tracking Queue **/
	private PriorityQueue<Job> prioritizedJobs = new PriorityQueue<Job>(64, new JobComparator());
	
	/** Job Execution Tracking Queue **/
	private PriorityQueue<Task> prioritizedTasks = new PriorityQueue<Task>(64, new TaskComparator());

	public MSLBL(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics,
			CloudSimWrapper cloudsim, boolean saveRuntimeData) {
		super(budget, deadline, dags, new CloudScheduler(cloudsim), new GeneralPurposeProvisioner(cloudsim),
				ensembleStatistics, cloudsim);

		vmPendingJob = new HashMap<VM, Job>();
		dagUnscheduledBudget = new HashMap<DAG, Map<Task, Double>>();
		dagBudget = new HashMap<DAG, Map<Task, Double>>();
		unscheduledTasks = new HashMap<DAG, List<Task>>();
		unfinishedTasks = new HashMap<DAG, List<Task>>();
	}
    
	protected class JobComparator implements Comparator<Job> {

        @Override
        public int compare(Job j1, Job j2) {
        	return Double.compare(j1.getTask().getEarliestStartTime(), j2.getTask().getEarliestStartTime());
        }
    }
	
	private void moveAllJobsToPriorityQueue(Queue<Job> jobs) {
        prioritizedJobs.addAll(jobs);
        jobs.clear();
    }
	
	@Override
	public void scheduleQueueJobs() {
		
		Queue<Job> queue = new LinkedList<Job>(getWorkflowEngine().getQueuedJobs());
		
		moveAllJobsToPriorityQueue(queue);
		
		//Scheduling the tasks
		while (!prioritizedJobs.isEmpty()) {
			Job job = prioritizedJobs.poll();
			Task task = job.getTask();
			DAG dag = job.getDAGJob().getDAG();
			double taskBudget = 0.0;
			double contDelay = 0.0;
			List<Task> unscheduledTask = new ArrayList<Task>();
			unscheduledTask = unscheduledTasks.get(dag);
			
			//check if budget has been distributed, if not create it
			if(!(dagUnscheduledBudget.containsKey(dag) && dagBudget.containsKey(dag))){
				double budget = dag.getBudget();
				Map<Task, Double> budgets = assignBudget(unscheduledTask, budget);
				dagUnscheduledBudget.put(dag, budgets);
				dagBudget.put(dag, budgets);
			}
			
			Map<Task, Double> budgetDistribution = new HashMap<Task, Double>();
			budgetDistribution = dagUnscheduledBudget.get(dag);
			taskBudget = budgetDistribution.get(task);
						
			System.out.println("******************");
			System.out.println("Scheduling task dag" + dag.getId() + "." + task.getId() + ", budget: "
					+ taskBudget);
			System.out.println("******************");

			// Check if there are free vms
			boolean foundFreeVm = false;
			if (!getWorkflowEngine().getFreeVMs().isEmpty()) {
				
				List<VM> freeVms = getFreeVms(dag);
				VM vm = findVMforTask(job, task.getDeadline(), taskBudget, freeVms);
				
				if (vm != null) { // If the vm is still null at this point then
									// we didn't find a free vm
					foundFreeVm = true;
					scheduleJob(job, vm, contDelay, true);
					unscheduledTask.remove(task);
				}
			}

			if (!foundFreeVm) { // There are no free VMs
				
				// Find VM type that can be afforded by the budget
				VMType vmType = findVMTypeForTask(job, task.getDeadline(), taskBudget);

				// Provision a new vm of the chosen type
				VM vm = provisioner.provisionResource(vmType, getWorkflowEngine());
				contDelay = 0.0;
				scheduleJob(job, vm, contDelay, false);
				unscheduledTask.remove(task);
			}
			unscheduledTasks.put(dag, unscheduledTask);
		}
		
		
		//This is the scheduling cycle
		//The algorithm will call this method either when a new workflow is arrived,
		//a job is finished or if it comes to a new scheduling cycle
		//getCloudsim().send(getWorkflowEngine().getId(), getWorkflowEngine().getId(), SCHEDULING_INTERVAL,
         //       WorkflowEvent.SCHEDULING_REQUEST, null);
	}

	@Override
	public void provisionResources() {
		provisioner.provisionResources(getWorkflowEngine());
	}

	@Override
	public void DAGSubmit(DAGJob dagJob) {
		System.out.println("******************");
		System.out.println("DAG Job Submitted");
		System.out.println("Start time: " + dagJob.getStartTime());
		System.out.println("DAG " + dagJob.getDAG().getId() + ": " + dagJob.getDAG().getName());
		System.out.println("******************");
		
		Map<Task, Double> runtimes = new HashMap<Task, Double>();
    	Map<Task, Double> earliestStartTimes = new HashMap<Task, Double>();
    	
    	Map<Task, Double> avgRuntimes = new HashMap<Task, Double>();
    	Map<Task, Integer> rankUpwards = new HashMap<Task, Integer>();
    	
		List<Task> unscheduledTask = new ArrayList<Task>();
		List<Task> unfinishedTask = new ArrayList<Task>();
		
		DAG dag = dagJob.getDAG();
		
		TopologicalOrder order = new TopologicalOrder(dag);
		TopologicalOrderReverse orderReverse = new TopologicalOrderReverse(dag);
    	VMType fastestVMType = environment.getFastestVM();
    	
    	for (Task task : order){
    		double runtime = environment.getPredictedRuntime(fastestVMType, task);
    		runtimes.put(task, runtime);
    		unscheduledTask.add(task);
			unfinishedTask.add(task);
    	}
    	
    	//Rank using Earliest Finish Time (EFT)
    	for (Task task : order) {
			double earliestStartTime = 0.0;
			for (Task parent : task.getParents()) {
				if(earliestStartTimes.get(parent) != null) {
					double pEarliestFinishTime = earliestStartTimes.get(parent) + runtimes.get(parent);
					earliestStartTime = Math.max(earliestStartTime, pEarliestFinishTime);
				} 
			}
			
			earliestStartTime = Math.max(earliestStartTime, CloudSim.clock());
			earliestStartTimes.put(task, earliestStartTime);
			task.setEarliestFinishTime(earliestStartTime + runtimes.get(task));	
		}
		
    	//Rank using Upward Rank
    	List<VMType> vmTypes = environment.getVmTypes();
    	for (Task task : order){
    		double avgRuntime = 0.0;
    		for(VMType vmType : vmTypes){
    			avgRuntime =+ environment.getPredictedRuntime(vmType, task);
    		}
    		avgRuntime = avgRuntime / vmTypes.size();
    		avgRuntimes.put(task, avgRuntime);
    	}
    	
    	for (Task task : orderReverse) {
			int rankUpward = (int) Math.ceil(avgRuntimes.get(task));
			for (Task child : task.getChildren()) {
				double cRankUpward = 0.0;
				if(rankUpwards.get(child) != null) {
					cRankUpward = rankUpwards.get(child) + avgRuntimes.get(task);
				}
				int tRankUpwardint = (int) Math.ceil(cRankUpward);
				rankUpward = Math.max(rankUpward, tRankUpwardint);
			}

			rankUpwards.put(task, rankUpward);
			task.setRank(rankUpward);	
		}
    	
		unscheduledTasks.put(dag, unscheduledTask);
		unfinishedTasks.put(dag, unfinishedTask);
		
		scheduleQueueJobs();
	}

	protected class TaskComparator implements Comparator<Task> {

        @Override
        public int compare(Task t1, Task t2) {
        	return Double.compare(t2.getRank(), t1.getRank());
        }
    }
	
	private void moveAllTasksToPriorityQueue(List<Task> tasks) {
        prioritizedTasks.addAll(tasks);
        tasks.clear();
    }
	
	private Map<Task, Double> assignBudget(List<Task> unscheduledTask, Double budget){
		Map<Task, Double> budgets = new HashMap<Task, Double>();
    	
    	moveAllTasksToPriorityQueue(unscheduledTask);
    	unscheduledTask.clear();
    	
    	while(!prioritizedTasks.isEmpty()){
    		unscheduledTask.add(prioritizedTasks.poll());
    	}
    	
    	budgets = BudgetDistribution.getBudgetDistributionMSLBL(unscheduledTask, budget, environment);
    	
    	return budgets;
    }
	
	@Override
	public void jobFinished(Job job) {
	
	}
	
	@Override
	public void DAGfinished(DAGJob dagJob) {
		dagFinished++;
		System.out.println("******************");
		System.out.println("DAG Job finished");
		System.out.println("DAG " + dagJob.getDAG().getId() + ": " + dagJob.getDAG().getName());
		System.out.println("Total DAG Finished: " + dagFinished);
		System.out.println("******************");

		// Deprovision resources if there are no more DAGs to run
		if (getWorkflowEngine().getDags().isEmpty()) {
			provisioner.deprovisionResources(getWorkflowEngine());
		}

	}

	@Override
	public void jobFailed(Job failedJob, Job retry) {
		// TODO Auto-generated method stub
		getCloudsim().log("WARNING: Job failed: " + failedJob.getID());
	}

	@Override
	public void vmLaunched(VM vm) {
		Job job = vmPendingJob.get(vm);
		double delay = 0.0;
		if (job != null) {
			vmPendingJob.remove(vm);
			double predictedRuntime = environment.getPredictedRuntime(vm.getVmType(), job.getTask());
			job.setEstimatedRuntime(predictedRuntime);
			scheduler.scheduleJob(job, vm, delay, getWorkflowEngine());
		} else {
			System.err.println("Warning! vm launched but pending job");
		}
	}

	/**
	 * Finds the fastest vm type that can finish a task within its budget
	 * considering the vm boot and terminate time of the vm type for meeting budget
	 * 
	 * @param task
	 * @param taskDeadline
	 * @param taskBudget
	 * @return
	 */
	private VMType findVMTypeForTask(Job job, double taskDeadline, double taskBudget) {

		List<VMType> vmTypes = environment.getVmTypes();
		Collections.sort(vmTypes, new VMTypeCostComparator());
		double largestCost = 0.0;
		
		VMType suitableVm = null;
		Task task = job.getTask();
		
		for (VMType vmType : vmTypes) {
			double runtime = environment.getPredictedRuntime(vmType, task);
			runtime += vmType.getProvisioningDelay().sample();
			double cost = environment.getCost(runtime, vmType);
			if (cost >= largestCost) {
				largestCost = cost;
				if (largestCost <= taskBudget){
					suitableVm = vmType;
				}
			} 
		}
		
		if (suitableVm == null) {
			suitableVm = environment.getCheapestVM();
		}
		
		return suitableVm;

	}

	/**
	 * Finds the fastest vm that can finish the task within budget
	 * 
	 * @param task
	 * @param inputDataVms
	 * @return
	 */
	private VM findVMforTask(Job job, double taskDeadline, double taskBudget, List<VM> vms) {
		
		double vmMips = 0.0;
		VM fastestVm = null;
		Task task = job.getTask();
		
		for (VM vm : vms) {
			double runtime = environment.getPredictedRuntime(vm.getVmType(), task);
			double cost = environment.getCost(runtime, vm.getVmType());
			
			if (cost <= taskBudget){
				if(vm.getVmType().getMips() > vmMips){
					vmMips = vm.getVmType().getMips();
					fastestVm = vm;
				}
			}
		}
		
	return fastestVm;
	}

	/**
	 * Finds the vm type that can finish the task the fastest, without
	 * considering cost or deadline, but it does consider vm boot time
	 * 
	 * @param task
	 * @return
	 */
	
	private List<VM> getFreeVms(DAG dag) {
		Set<VM> vms = getWorkflowEngine().getFreeVMs();
		List<VM> freeVms = new ArrayList<VM>();
		for (VM vm : vms) {
			// make sure the VM is really free...sometimes the vm list in the
			// engine is not updated on time
			if (vm.getRunningJobs().isEmpty() && vm.getWaitingInputJobs().isEmpty()) {
				freeVms.add(vm);
			}
		}
		return freeVms;
	}
	
	private void scheduleJob(Job job, VM vm, Double delay, boolean submit) {
		getWorkflowEngine().getQueuedJobs().remove(job);
		// Submit is true when the VM is already up and running
		if (submit) {
			double predictedRuntime = environment.getPredictedRuntimeOnVM(vm, job.getTask());
			job.setEstimatedRuntime(predictedRuntime);
			scheduler.scheduleJob(job, vm, delay, getWorkflowEngine());
		} else {// waiting for the vm to launch
			if (vmPendingJob.containsKey(vm)) {
				System.err.println("WARNING! trying to assign more than one job at a time to a vm");
			} else {
				vmPendingJob.put(vm, job);
			}
		}
	}

	@Override
	public void DAGfinished() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void vmTerminated(VM vm) {
		// TODO Auto-generated method stub
		
	}

}
