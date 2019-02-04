package cws.core.scheduler;

import cws.core.Scheduler;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

public class CloudScheduler extends CWSSimEntity implements Scheduler {
	
	private Environment environment;

	public CloudScheduler(CloudSimWrapper cloudsim) {
		super("CloudScheduler", cloudsim);
	}

	@Override
	public void scheduleJobs(WorkflowEngine engine) {
		//Do nothing, this logic goes in the actual algorithm so we can combine it with resource
		//provisioning if we wanted to
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}
	
	
	//Hilman: edit here for container provisioning delay
	public void scheduleJob(Job job, VM vm, Double delay, WorkflowEngine engine) {
		
		markVMAsBusy(engine, vm);

		job.setVM(vm);

		sendJobToVM(engine, vm, delay, job);
	}
	
	private void markVMAsBusy(WorkflowEngine engine, VM vm) {
		//this is done in the engine when a job starts too...
		int idleTime = 0;
		vm.setIdleTime(idleTime);
        if(engine.getFreeVMs().remove(vm)) {
        	engine.getBusyVMs().add(vm);
        }
    }

	private void sendJobToVM(WorkflowEngine engine, VM vm, Double delay, Job job) {
		getCloudsim().send(engine.getId(), vm.getId(), delay,
				WorkflowEvent.JOB_SUBMIT, job);
		getCloudsim().log(
				"Submitting " + job.toString() + " to VM "
						+ job.getVM().getId());
	}
	
	

}
