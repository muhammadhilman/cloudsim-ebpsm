package cws.core.provisioner;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.Job;

public class OneTaskOneVMProvisioner extends CloudProvisioner {

	public OneTaskOneVMProvisioner(CloudSimWrapper cloudsim) {
		super(cloudsim);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void provisionResources(WorkflowEngine engine) {
		// TODO Auto-generated method stub
		getCloudsim().send(engine.getId(), engine.getId(),
				PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST, null);
	}

	@Override
	public void deprovisionResources(WorkflowEngine engine) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public VM provisionResource(Job job, WorkflowEngine engine) {
		// TODO Auto-generated method stub		
        return null;
		
	}

	@Override
	public VM provisionResource(VMType vmType, WorkflowEngine engine) {
		// TODO Auto-generated method stub
		VM vm = VMFactory.createVM(vmType, getCloudsim());
		getCloudsim().log("Starting VM: " + vm.getId() + ". MIPS: " +  vmType.getMips());
        getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
		
		return vm;
	}

	@Override
	public void deprovisionResource(VM vm, WorkflowEngine engine) {
		// TODO Auto-generated method stub
		getCloudsim().log("Terminating VM: " + vm.getId());
        getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
	}

}
