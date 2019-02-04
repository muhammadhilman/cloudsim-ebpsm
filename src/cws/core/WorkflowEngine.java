package cws.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import cws.core.dag.DAGJob;
import cws.core.jobs.Job;
import cws.core.jobs.JobListener;

/**
 * The workflow engine is an entity that executes workflows by scheduling their
 * tasks on VMs.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public interface WorkflowEngine {
    public static int next_id = 0;

    public double getCost();

    public double getDeadline();

    public double getBudget();
    
    public int getQueueLength();

    public void setQueueLength(int queueLength);
    
    public Queue<Job> getQueuedJobs();

    public List<VM> getAvailableVMs();

    public Set<VM> getFreeVMs();

    public Set<VM> getBusyVMs();

    public void addJobListener(JobListener l);

    public void removeJobListener(JobListener l);
    
    public int getId();

	public void queueJob(Job retry);
	
	public void queueReadyJobs(DAGJob dagJob);

	public LinkedList<DAGJob> getDags();
}
