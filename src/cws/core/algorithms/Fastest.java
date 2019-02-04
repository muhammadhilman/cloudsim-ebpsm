package cws.core.algorithms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.jobs.Job;
import cws.core.provisioner.OneTaskOneVMProvisioner;
import cws.core.scheduler.CloudScheduler;

public class Fastest extends CombinedDynamicAlgorithm {
	
	private Map<Job, VM> jobsProvisioned = new HashMap<Job, VM>();;
	private Map<VM, Job> jobsScheduled = new HashMap<VM, Job>();
	private VMType vmType = null;
	
	
	public Fastest(double budget, double deadline, List<DAG> dags, AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim, boolean saveRuntimeData) {
		super(budget, deadline, dags, new CloudScheduler(cloudsim), new OneTaskOneVMProvisioner(cloudsim), ensembleStatistics, cloudsim);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void scheduleQueueJobs() {
		// TODO Auto-generated method stub
		WorkflowEngine engine = getWorkflowEngine();
		Queue<Job> jobs = engine.getQueuedJobs();
		
		for (Job job : jobs){
			if(!jobsProvisioned.containsKey(job)){
				vmType = environment.getFastestVM();
				VM vm = provisioner.provisionResource(vmType, engine);
				jobsProvisioned.put(job, vm);
				jobsScheduled.put(vm, job);
			}
		}
	}
	
	@Override
	public void provisionResources() {
		// TODO Auto-generated method stub
		provisioner.provisionResources(getWorkflowEngine());
	}
	
	@Override
	public void DAGSubmit(DAGJob dagJob) {
		// TODO Auto-generated method stub
		scheduleQueueJobs();
	}

	@Override
	public void jobFinished(Job job) {
		// TODO Auto-generated method stub
		if(job.getState().equals(Job.State.TERMINATED)){
			provisioner.deprovisionResource(job.getVM(), getWorkflowEngine());
		}
	}

	@Override
	public void DAGfinished(DAGJob dagJob) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jobFailed(Job failedJob, Job retry) {
		// TODO Auto-generated method stub
		VM vm = failedJob.getVM();
		double delay = 0.0;
		scheduler.scheduleJob(retry, vm, delay, getWorkflowEngine());
	}

	@Override
	public void vmLaunched(VM vm) {
		// TODO Auto-generated method stub
		double delay = 0.0;
		WorkflowEngine engine = getWorkflowEngine();
		Queue<Job> jobs = engine.getQueuedJobs();
		Job job = jobsScheduled.get(vm);
		jobs.remove(job);
		scheduler.scheduleJob(job, vm, delay, engine);
		scheduleQueueJobs();
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
