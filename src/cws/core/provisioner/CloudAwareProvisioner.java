package cws.core.provisioner;

import cws.core.Cloud;
import cws.core.Provisioner;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.engine.Environment;

public abstract class CloudAwareProvisioner extends CWSSimEntity implements Provisioner {
    public static final double DEFAULT_AUTOSCALING_FACTOR = 2.0;
    protected static final double PROVISIONER_INTERVAL = 1.0;

    private Cloud cloud;
    protected Environment environment;

    // maximum autoscaling factor over initial number of provisioned VMs
    protected double maxScaling;

    public CloudAwareProvisioner(CloudSimWrapper cloudsim) {
        this(DEFAULT_AUTOSCALING_FACTOR, cloudsim);
    }

    public CloudAwareProvisioner(double maxScaling, CloudSimWrapper cloudsim) {
        super("CloudAwareProvisioner", cloudsim);
        this.maxScaling = maxScaling;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public void setMaxScaling(double maxScaling) {
        this.maxScaling = maxScaling;
    }

    public double getMaxScaling() {
        return maxScaling;
    }

    public void setCloud(Cloud cloud) {
        this.cloud = cloud;
    }

    public Cloud getCloud() {
        return cloud;
    }
}
