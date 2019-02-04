package cws.core.provisioner;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.Job;

public class SingleVMProvisioner extends CloudProvisioner {

	private boolean provisionedVM = false;
	    
	public SingleVMProvisioner(CloudSimWrapper cloudsim) {
		super(cloudsim);
	}

	@Override
	public void provisionResources(WorkflowEngine engine) {
		if(!provisionedVM) {
		// We only want to provision a single VM for all tasks
		// check if a VM has already been provisioned, if not then provision it
		//if (engine.getAvailableVMs().isEmpty()) {
			List<VMType> vmTypes = environment.getVmTypes();
						
			VMType slowestType = null;
			double slowestMips = Double.MAX_VALUE;
			for (VMType vmType : vmTypes) {
				if(vmType.getMips() < slowestMips) {
					slowestMips = vmType.getMips();
					slowestType = vmType;
				}
			}
			
			VM vm = VMFactory.createVM(slowestType, getCloudsim());
			provisionedVM = true;
	        getCloudsim().log("Starting VM: " + vm.getId() + ". MIPS: " +  slowestType.getMips());
	        getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
		}
		
		//If the provisioner works separately from the scheduler then include provisioning logic here, this is called
		//every provisioning cycle. If not, then include provisioning logic in the actual algorithm.
		getCloudsim().send(engine.getId(), engine.getId(),
				PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST, null);
	}

	@Override
	public void deprovisionResources(WorkflowEngine engine) {
		Set<VM> freeVMs = engine.getFreeVMs();
        Iterator<VM> vmIt = freeVMs.iterator();
        while (vmIt.hasNext()) {
            VM vm = vmIt.next();
            vmIt.remove();
            deprovisionResource(vm, engine);
        }
	}

	@Override
	public VM provisionResource(Job job, WorkflowEngine engine) {
		return null;
	}

	@Override
	public void deprovisionResource(VM vm, WorkflowEngine engine) {
		getCloudsim().log("Terminating VM: " + vm.getId());
        getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
	}

	@Override
	public VM provisionResource(VMType vmType, WorkflowEngine engine) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
