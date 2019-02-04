package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.VM;
import cws.core.WorkflowEngine;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.engine.Environment;
import cws.core.jobs.Job;

public abstract class CloudProvisioner extends CWSSimEntity implements Provisioner {
	
	protected static final double PROVISIONER_INTERVAL = 1.0;

    private Cloud cloud;
    protected Environment environment;


    public CloudProvisioner(CloudSimWrapper cloudsim) {
    	super("CloudAwareProvisioner", cloudsim);
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    public Cloud getCloud() {
        return cloud;
    }
    
    public abstract void provisionResources(WorkflowEngine engine);
    public abstract void deprovisionResources(WorkflowEngine engine);
    public abstract VM provisionResource(Job job, WorkflowEngine engine);
    public abstract VM provisionResource(VMType vmType, WorkflowEngine engine);
    public abstract void deprovisionResource(VM vm, WorkflowEngine engine);
}
