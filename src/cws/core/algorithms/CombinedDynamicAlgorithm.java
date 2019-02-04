package cws.core.algorithms;

import java.util.List;

import cws.core.Cloud;
import cws.core.EnsembleManager;
import cws.core.VM;
import cws.core.WorkflowEngineCombinedRPSchedNoContainer;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.jobs.Job;
import cws.core.provisioner.CloudProvisioner;
import cws.core.scheduler.CloudScheduler;

/**
 * Schedules a single DAG dynamically, doesn't make any decisions on initial VMs or provisioning, that
 * is left up to the provisioner to do
 * @author maria
 *
 */
public abstract class CombinedDynamicAlgorithm extends Algorithm {
	
	protected static final double SCHEDULING_INTERVAL = 10.0;
	
	protected CloudScheduler scheduler;
    protected CloudProvisioner provisioner;
    protected boolean saveRuntimeData;
	
	public CombinedDynamicAlgorithm(double budget, double deadline,
			List<DAG> dags, CloudScheduler scheduler,
			CloudProvisioner provisioner,
			AlgorithmStatistics ensembleStatistics, CloudSimWrapper cloudsim) {
		super(budget, deadline, dags, ensembleStatistics, cloudsim);
		this.provisioner = provisioner;
        this.scheduler = scheduler;
	}

	@Override
	protected void simulateInternal() {
		prepareEnvironment();
        getCloudsim().startSimulation();
	}
	
	private void prepareEnvironment() {
        provisioner.setEnvironment(environment);
        scheduler.setEnvironment(environment);

        setCloud(new Cloud(getCloudsim()));
        provisioner.setCloud(getCloud());
        
        setWorkflowEngine(new WorkflowEngineCombinedRPSchedNoContainer(this, getBudget(), getDeadline(), getCloudsim()));

        //The ensemble manager submits the dags to the wf engine for execution
        setEnsembleManager(new EnsembleManager(getAllDags(), getWorkflowEngine(), getCloudsim()));
    }
	
	@Override
	public long getPlanningnWallTime() {
		return 0;
	}

	public abstract void scheduleQueueJobs();
	public abstract void provisionResources();
	public abstract void DAGSubmit(DAGJob dagJob);
	public abstract void jobFinished(Job job);
	public abstract void DAGfinished(DAGJob dagJob);
	public abstract void jobFailed(Job failedJob, Job retry);
	public abstract void vmLaunched(VM vm);
	public abstract void vmTerminated(VM vm);
	public abstract void DAGfinished();

}
