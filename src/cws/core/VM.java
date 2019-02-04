package cws.core;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.Task;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.IdentityRuntimeDistribution;
import cws.core.jobs.Job;
import cws.core.jobs.RuntimeDistribution;
import cws.core.transfer.Port;

/**
 * A VM is a virtual machine that executes Jobs.
 * 
 * It has a number of cores, and each core has a certain power measured
 * in MIPS (millions of instructions per second).
 * 
 * It has an input Port that is used to transfer data to the VM, and an output
 * Port that is used to transfer data from the VM. Both ports have the same
 * bandwidth.
 * 
 * Jobs can be queued and are executed in FIFO order. The scheduling is
 * space shared.
 * 
 * It has a price per billing unit. The cost of a VM is computed by multiplying the
 * runtime in billing units by the billing unit price. The runtime is rounded up to the
 * nearest billing unit for this calculation.
 * 
 * Each VM has a provisioning delay between when it is launched and when it
 * is ready, and a deprovisioning delay between when it is terminated and
 * when the provider stops charging for it.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class VM extends CWSSimEntity {

    private static int nextId = 0;
    
    /** */
    private int idleTime;
    
    /** Contains VM parameters like cores number, price for billing unit **/
    private VMType vmType;

    /** The SimEntity that owns this VM */
    private int owner;

    /** The Cloud that runs this VM */
    private int cloud;

    /** Current idle cores */
    private int idleCores;

    /** Queue of jobs submitted to this VM */
    private LinkedList<Job> jobs;

    /** Set of jobs currently running */
    private Set<Job> runningJobs;
    
    /** Set of jobs currently waiting for input to transfer */
    private Set<Job> waitingInputJobs;

    /** Time that the VM was launched */
    private double launchTime;

    /** Time that the VM was terminated */
    private double terminateTime;

    /** Has this VM been terminated? */
    private boolean isTerminated;

    /** Has this VM been started? */
    private boolean isLaunched;

    /** Number of CPU seconds consumed by jobs on this VM */
    private double cpuSecondsConsumed;

    /** Varies the actual runtime of tasks according to the specified distribution */
    private RuntimeDistribution runtimeDistribution = new IdentityRuntimeDistribution();

    /** Varies the failure rate of tasks according to a specified distribution */
    private FailureModel failureModel = new FailureModel (0, 0.0);
    
    /** Varies the actual performance of the VM to the specified distribution */
    private PerformanceVariationDistribution pvDistribution = new IdentityPerformanceVariationDistribution();
    
    /** Input port used to transfer data into the VM */
    private Port inputPort;
    
    /** Output port used to trasnfer data out of the VM */
    private Port outputPort;
    
    /** Available bandwidth 1 Gbps (in Mbps). Temporary hack, should be moved to config file */
    private static double BANDWIDTH = 1000.0;

    public VM(VMType vmType, CloudSimWrapper cloudsim) {
        super("VM" + (nextId++), cloudsim);
        this.idleTime = 0;
        this.vmType = vmType;
        this.jobs = new LinkedList<Job>();
        this.runningJobs = new HashSet<Job>();
        waitingInputJobs = new HashSet<Job>();
        this.idleCores = vmType.getCores();
        this.launchTime = -1.0;
        this.terminateTime = -1.0;
        this.cpuSecondsConsumed = 0.0;
        this.isTerminated = false;
        this.isLaunched = false;
        this.inputPort = new Port(BANDWIDTH);
        this.outputPort = new Port(BANDWIDTH);
    }

    /**
     * Runtime of the VM in seconds. If the VM has not been launched, then
     * the result is 0. If the VM is not terminated, then we use the current
     * simulation time as the termination time. After the VM is terminated
     * the runtime does not change.
     */
    public double getRuntime() {
        if (launchTime < 0)
            return 0.0;
        else if (terminateTime < 0)
            return getCloudsim().clock() - launchTime;
        else
            return terminateTime - launchTime;
    }

    /**
     * Compute the total cost of this VM. This is computed by taking the
     * runtime, rounding it up to the nearest whole billing unit, and multiplying
     * by the billing unit price.
     */
    public double getCost() {
        double billingUnits = getRuntime() / vmType.getBillingTimeInSeconds();
        double fullBillingUnits = Math.ceil(billingUnits);
        return fullBillingUnits * vmType.getPriceForBillingUnit();
    }

    public double getCPUSecondsConsumed() {
        return cpuSecondsConsumed;
    }

    /** cpu_seconds / (runtime * cores) */
    public double getUtilization() {
        double totalCPUSeconds = getRuntime() * vmType.getCores();
        return cpuSecondsConsumed / totalCPUSeconds;
    }

    @Override
    public void processEvent(CWSSimEvent ev) {
        if (!isTerminated) {
            switch (ev.getTag()) {
            case WorkflowEvent.VM_LAUNCH:
                launchVM();
                break;
            case WorkflowEvent.VM_TERMINATE:
                terminateVM();
                break;
            case WorkflowEvent.JOB_SUBMIT:
                jobSubmit((Job) ev.getData());
                break;
            case WorkflowEvent.JOB_FINISHED:
                jobFinish((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_BEFORE_TRANSFERS_COMPLETED:
                allInputsTrasferred((Job) ev.getData());
                break;
            case WorkflowEvent.STORAGE_ALL_AFTER_TRANSFERS_COMPLETED:
                allOutputsTransferred((Job) ev.getData());
                break;
            default:
                throw new UnknownWorkflowEventException("Unknown event: " + ev);
            }
        } else {
            if (ev.getTag() == WorkflowEvent.VM_LAUNCH || ev.getTag() == WorkflowEvent.JOB_SUBMIT) {
                throw new IllegalStateException("Attempted to send launch or submit event to terminated VM:"
                        + this.getId());
            }
        }
    }

    private void launchVM() {
        if (this.isLaunched) {
            throw new IllegalStateException("Attempted to launch already launched VM:" + this.getId());
        }
        this.isLaunched = true;
        getCloudsim().log(String.format("VM %d started", getId()));
    }

    private void terminateVM() {
        // Can no longer accept jobs
        isTerminated = true;

        // Log termination only for running jobs
        for (Job runningJob : runningJobs) {
            getCloudsim().log("Terminating job " + runningJob.getID() + " on VM " + runningJob.getVM().getId());
        }

        // Log that queued jobs were not executed
        for (Job queuedJob : jobs) {
            getCloudsim().log("Removing job " + queuedJob.getID() + " from queue on VM " + queuedJob.getVM().getId());
        }

        // Move running jobs back to the queue...
        jobs.addAll(runningJobs);
        runningJobs.clear();

        // ... and fail all queued jobs
        for (Job job : jobs) {
            job.setResult(Job.Result.FAILURE);
            getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);
        }

        // Reset dynamic state
        jobs.clear();
        idleCores = vmType.getCores();
        getCloudsim().log(String.format("VM %d terminated", getId()));
    }

    public void removeJobsFromQueue(List<Job> jobsToRemove) {
    	for(Job toRemove : jobsToRemove) {
    		Task t = toRemove.getTask();
    		Iterator<Job> it = jobs.iterator();
    		while(it.hasNext()) {
    			Job j = it.next();
    			if(j.getTask().getId().equals(t.getId())) {
    				// Log that queued jobs were not executed
    		        getCloudsim().log("Removing job " + j.getID() + " from queue on VM " + j.getVM().getId());  
            		it.remove();
    			}
    		}
    	}
    	/*if(jobs.isEmpty() && runningJobs.isEmpty()) {
    		idleCores = vmType.getCores();
    	}*/
    }
    
    public Set<Job> getRunningJobs() {
    	return runningJobs;
    }
    
    public Set<Job> getWaitingInputJobs() {
    	return waitingInputJobs;
    }
    
    public void cancelRunningJobs() {
    	 // Log termination only for running jobs
        for (Job runningJob : runningJobs) {
            getCloudsim().log("Terminating job " + runningJob.getID() + " on VM " + runningJob.getVM().getId());
            runningJob.setResult(Job.Result.FAILURE);
            getCloudsim().send(getId(), runningJob.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, runningJob);
            
        }
        if(!runningJobs.isEmpty()) {
        	runningJobs.clear();
        	idleCores = vmType.getCores();
        }
    }
    
    private void jobSubmit(Job job) {
        job.setSubmitTime(getCloudsim().clock());
        job.setState(Job.State.IDLE);
        job.setVM(this);

        // Queue the job
        jobs.add(job);

        // This shouldn't do anything if the VM is busy
        startJobs();
    }

    private void allInputsTrasferred(Job job) {
        if (runningJobs.contains(job)) {
            throw new IllegalStateException("Job already running: " + job);
        }
        // add it to the running set
        runningJobs.add(job);
        waitingInputJobs.remove(job);

        // Compute the duration of the job on this VM
        double size = job.getTask().getSize();
        
        //Introduce performance variation
        double performanceVariation = pvDistribution
				.getPerformanceVariation();
		getCloudsim().log(
				String.format("Performance degraded on VM %s by %s", job
						.getVM().getId(), 1.0 - performanceVariation));
		double mipsAfterDegradation = vmType.getMips()
				* performanceVariation;
        
        double predictedRuntime = size / mipsAfterDegradation;

        // Compute actual runtime
        double actualRuntime = this.runtimeDistribution.getActualRuntime(predictedRuntime);

        // Decide whether the job succeeded or failed
        if (failureModel.failureOccurred()) {
            job.setResult(Job.Result.FAILURE);

            // How long did it take to fail?
            actualRuntime = failureModel.runtimeBeforeFailure(actualRuntime);
        } else {
            job.setResult(Job.Result.SUCCESS);
        }

        getCloudsim().log(
                String.format("Starting computational part of job %s (task_id = %s, workflow = %s) on VM %s",
                        job.getID(), job.getTask().getId(), job.getDAGJob().getDAG().getId(), job.getVM().getId()));

        getCloudsim().send(getId(), getId(), actualRuntime, WorkflowEvent.JOB_FINISHED, job);
    }

    private void allOutputsTransferred(Job job) {
        if (!runningJobs.contains(job) || job.getState() != Job.State.RUNNING) {
            throw new IllegalStateException("Outputs of non-running job transferred:" + job.getID());
        }
        
     // The core that was running the job is now free
        idleCores++;
        
        //remove from the running set
        runningJobs.remove(job);

        // Complete the job
        job.setFinishTime(getCloudsim().clock());
        job.setState(Job.State.TERMINATED);

        // Increment the usage
        cpuSecondsConsumed += job.getDuration();

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, job);

        // We may be able to start more jobs now
        startJobs();
    }

    private void jobStart(Job job) {
        if (job.getState() != Job.State.IDLE) {
            throw new IllegalStateException("Attempted to start non-idle job:" + job.getID());
        } else if (idleCores != 1) {
            // NOTE(bryk): Here we assume that VMs always have only one core. It should be changed once we enable more
            // cores in VMs.
            throw new IllegalStateException("Number of idle cores is not 1, actual number:" + idleCores);
        }
        getCloudsim().log("Starting " + job.toString() + " on VM " + job.getVM().getId());
        // The job is now running
        job.setStartTime(getCloudsim().clock());
        job.setState(Job.State.RUNNING);
        waitingInputJobs.add(job);

        // Tell the owner
        getCloudsim().send(getId(), job.getOwner(), 0.0, WorkflowEvent.JOB_STARTED, job);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_BEFORE_TASK_START, job);

        // One core is now busy running the job
        idleCores--;
    }

    private void jobFinish(Job job) {
        if (job.getState() != Job.State.RUNNING) {
            throw new RuntimeException("Non-running job finished:" + job.getID());
        }

        String msg = String.format("Computational part of job %s "
                + "(task_id = %s, workflow = %s, retry = %s) on VM %s finished", job.getID(), job.getTask().getId(),
                job.getDAGJob().getDAG().getId(), job.isRetry(), job.getVM().getId());
        getCloudsim().log(msg);

        getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                WorkflowEvent.STORAGE_AFTER_TASK_COMPLETED, job);
    }

    private void startJobs() {
        // While there are still idle jobs and cores
        while (jobs.size() > 0 && idleCores > 0) {
            // Start the next job in the queue
            jobStart(jobs.poll());
        }
    }

    public int getOwner() {
        return owner;
    }

    public void setOwner(int owner) {
        this.owner = owner;
    }

    public int getCloud() {
        return cloud;
    }

    public void setCloud(int cloud) {
        this.cloud = cloud;
    }

    public int getIdleCores() {
        return idleCores;
    }

    public Task[] getQueuedJobs() {
        return jobs.toArray(new Task[0]);
    }

    public LinkedList<Job> getQueuedJobsList() {
    	return jobs;
    }
    
    public int getQueueLength() {
        return jobs.size();
    }

    public void setLaunchTime(double launchTime) {
        this.launchTime = launchTime;
    }

    public double getLaunchTime() {
        return launchTime;
    }
    
    public void setIdleTime(int idleTime){
    	this.idleTime = idleTime;
    }
    
    public int getIdleTime(){
    	return idleTime;
    }
    
    public void setTerminateTime(double terminateTime) {
        this.terminateTime = terminateTime;
    }

    public double getTerminateTime() {
        return terminateTime;
    }

    public VMType getVmType() {
        return vmType;
    }

    public RuntimeDistribution getRuntimeDistribution() {
        return runtimeDistribution;
    }

    public void setRuntimeDistribution(RuntimeDistribution runtimeDistribution) {
        this.runtimeDistribution = runtimeDistribution;
    }

    public FailureModel getFailureModel() {
        return failureModel;
    }

    public void setFailureModel(FailureModel failureModel) {
        this.failureModel = failureModel;
    }

    public void setVmType(VMType vmType) {
        this.vmType = vmType;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public double getProvisioningDelay() {
        return vmType.getProvisioningDelay().sample();
    }

    public double getDeprovisioningDelay() {
        return vmType.getDeprovisioningDelay().sample();
    }

    public Port getInputPort() {
		return inputPort;
	}

	public Port getOutputPort() {
		return outputPort;
	}
    
	public double getBandwidth() {
		return BANDWIDTH;
	}

	public PerformanceVariationDistribution getPvDistribution() {
		return pvDistribution;
	}

	public void setPvDistribution(PerformanceVariationDistribution pvDistribution) {
		this.pvDistribution = pvDistribution;
	}
	
	public void cancelRunningJob(Job running) {
		 // Log termination only for running jobs
		Iterator<Job> iterator = runningJobs.iterator();
		while(iterator.hasNext()) {
			Job runningJob = iterator.next();
			if(running.getID() == runningJob.getID()) {
       		getCloudsim().log("Terminating job " + runningJob.getID() + " on VM " + runningJob.getVM().getId());
       		runningJob.setResult(Job.Result.FAILURE);
       		getCloudsim().send(getId(), runningJob.getOwner(), 0.0, WorkflowEvent.JOB_FINISHED, runningJob);
       		idleCores++;
       		iterator.remove();
       		break;
			}
		}
	}
}
