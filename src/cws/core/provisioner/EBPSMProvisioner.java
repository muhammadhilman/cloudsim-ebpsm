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

public class EBPSMProvisioner extends CloudProvisioner {

    // below this utilization threshold we start deprovisioning vms
    private static final double LOWER_THRESHOLD = 100;
    
    // higher that this threshold we terminate vms
    private static final int IDLE_THRESHOLD = 20;
	
	public EBPSMProvisioner(CloudSimWrapper cloudsim) {
		super(cloudsim);
	}

	@Override
	public void provisionResources(WorkflowEngine engine) {
		//If the provisioner works separately from the scheduler then include provisioning logic here, this is called
		//every provisioning cycle. If not, then include provisioning logic in the actual algorithm.
		
		//TODO check here which free vms are approaching their billing cycle and need to be
		//deprovisioned
		
		// running vms are free + busy
        Set<VM> runningVMs = new HashSet<VM>(engine.getFreeVMs());
        runningVMs.addAll(engine.getBusyVMs());

        int numVMsRunning = runningVMs.size();
		
		// find free VMs that will complete their billing unit during the next provisioning cycle
        Set<VM> completingVMs = new HashSet<VM>();
      
        for (VM vm : engine.getFreeVMs()) {
            int idleTime = vm.getIdleTime();
            if(idleTime >= IDLE_THRESHOLD){
            	completingVMs.add(vm);
            } else {
            	idleTime++;
            	vm.setIdleTime(idleTime);
            }
        }
        
        int numVMsCompleting = completingVMs.size();
        
        if (engine.getFreeVMs().size() + engine.getBusyVMs().size() > 0){
        	// compute utilization
            double numFreeVMS = engine.getFreeVMs().size();
            double numBusyVMs = engine.getBusyVMs().size();
            double utilization = numBusyVMs / (numFreeVMS + numBusyVMs);

            if (!(utilization >= 0.0)) {
                throw new RuntimeException("Utilization is not >= 0.0");
            }

            getCloudsim().log(
                    "Provisioner: utilization: " + utilization + " number of instances: "
                            + numVMsRunning + ", number of instances completing: " + numVMsCompleting);
                        
            //if utilization is lower than threshold, terminate all of free && completingVMs
            if (utilization < LOWER_THRESHOLD) {
                // select VMs to terminate from CompletingVMs
                Set<VM> toTerminate = new HashSet<VM>();
                for (VM vm : completingVMs){
                  	if(vm.getQueueLength() > 0 || vm.getRunningJobs().size() > 0){
                   		//still have jobs queued and running, do nothing
                   	} else {
                   		toTerminate.add(vm);
                   	}
                }
                Set<VM> terminated = terminateInstances(engine, toTerminate);
                completingVMs.removeAll(terminated);
            }
        }

        // some instances may be still running so we want to be invoked again to stop them
        //before they reach full billing unit
        getCloudsim().send(engine.getId(), engine.getId(), PROVISIONER_INTERVAL,
                    WorkflowEvent.PROVISIONING_REQUEST, null);
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