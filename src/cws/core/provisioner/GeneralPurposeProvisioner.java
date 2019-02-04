package cws.core.provisioner;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.jobs.Job;

public class GeneralPurposeProvisioner extends CloudProvisioner {

	    
	public GeneralPurposeProvisioner(CloudSimWrapper cloudsim) {
		super(cloudsim);
	}

	@Override
	public void provisionResources(WorkflowEngine engine) {
		//If the provisioner works separately from the scheduler then include provisioning logic here, this is called
		//every provisioning cycle. If not, then include provisioning logic in the actual algorithm.
		
		//TODO check here which free vms are approaching their billing cycle and need to be
		//deprovisioned
		
		// find free VMs that will complete their billing unit during the next provisioning cycle
        Set<VM> completingVMs = new HashSet<VM>();

        for (VM vm : engine.getFreeVMs()) {
            double vmRuntime = vm.getRuntime();

            // full billing units (rounded up)
            double vmBillingUnits = Math.ceil(vmRuntime / vm.getVmType().getBillingTimeInSeconds());

            // seconds till next full unit
            double secondsRemaining = vmBillingUnits * vm.getVmType().getBillingTimeInSeconds() - vmRuntime;

            // we add delay estimate to include also the deprovisioning time
            //If the next provisioning cycle is due to happen after secondsRemaining then deprovision
            //the free vm now to avoid going into an extra billing periond
           if(secondsRemaining <= environment.getVMDeprovisioningDelayEstimation(vm.getVmType()) ||
            		secondsRemaining < PROVISIONER_INTERVAL){
        	   // if secondsRemaining < 0 we are already on the next billing period so lets leave it hoping it'll get used
        	   if(secondsRemaining - environment.getVMDeprovisioningDelayEstimation(vm.getVmType()) >= 0)
        		   completingVMs.add(vm);
            }
        }
        
        // start terminating vms
        Set<VM> terminated = terminateInstances(engine, completingVMs);
        
        // remove terminated vms from free set
        engine.getFreeVMs().removeAll(terminated);

        // some instances may be still running so we want to be invoked again to stop them before they reach full
        // billing unit
        //if (engine.getFreeVMs().size() + engine.getBusyVMs().size() > 0) {
            getCloudsim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL,
                    WorkflowEvent.PROVISIONING_REQUEST, null);
    	//}

		//getCloudsim().send(engine.getId(), engine.getId(),
		//		PROVISIONER_INTERVAL, WorkflowEvent.PROVISIONING_REQUEST, null);
	}

	@Override
	public void deprovisionResources(WorkflowEngine engine) {
		
		for(VM free : engine.getFreeVMs()) {
			deprovisionResource(free, engine);
		} 
		
		for(VM busy : engine.getBusyVMs()) {
			getCloudsim().log("WARNING: Just deprovisioned a Busy VM: " + busy.getId());
			deprovisionResource(busy, engine);
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
        VM vm = VMFactory.createVM(vmType, getCloudsim());

        getCloudsim().log("Starting VM: " + vm.getId() + ". MIPS: " +  vmType.getMips());
        
        //getCloudsim().log("Starting VM: " + vm.getId());
        getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_LAUNCH, vm);
        return vm;
	}
	

    /**
     * This method terminates instances but only the ones
     * that are close to the full billing unit of operation.
     * Thus this method has to be invoked several times
     * to effectively terminate all the instances.
     * The method modifies the given vmSet by removing the terminated Vms.
     * 
     * @param engine
     * @param vmSet
     * @return set of VMs that were terminated
     */
    private Set<VM> terminateInstances(WorkflowEngine engine, Set<VM> vmSet) {
        Set<VM> removed = new HashSet<VM>();
        Iterator<VM> vmIt = vmSet.iterator();

        while (vmIt.hasNext()) {
            VM vm = vmIt.next();
            vmIt.remove();
            removed.add(vm);
            getCloudsim().log("Terminating VM: " + vm.getId());
            getCloudsim().send(engine.getId(), getCloud().getId(), 0.0, WorkflowEvent.VM_TERMINATE, vm);
        }
        return removed;
    }
}
