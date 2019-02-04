package cws.core.dag;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import cws.core.jobs.Job;


/**
 * This class records information about the execution of a DAG, including the
 * state of all tasks.
 * 
 * @author Gideon Juve <juve@usc.edu>
 */
public class DAGJob {
    /** The entity that owns the DAG */
    private int owner;

    /** The DAG being executed */
    private DAG dag;

    /** Set of tasks that have been released */
    private Set<Task> releasedTasks;

    /** Set of tasks that are finished */
    private Set<Task> completedTasks;
    
    /** Maria: Set of jobs that finished, to keep track of the VM where they ran */
    private Set<Job> completedJobs;

    /** List of all tasks that are ready but have not been claimed */
    private LinkedList<Task> queue;

    /** Workflow priority */
    private int priority;
    
    /** Time when this DAG should start running, used for simulating continuous workload*/
    private double startTime;
    
    /** Workflow SpareBudget */
    private double dagSpareBudget;

    public DAGJob(DAG dag, int owner, double startTime) {
        this.dag = dag;
        this.owner = owner;
        this.queue = new LinkedList<Task>();
        this.releasedTasks = new HashSet<Task>();
        this.completedTasks = new HashSet<Task>();
        this.completedJobs = new HashSet<Job>();
        this.startTime = startTime;

        // Release all root tasks
        for (String tid : dag.getTasks()) {
            Task t = dag.getTaskById(tid);
            if (t.getParents().size() == 0 || (t.getParents().size() == 1 && t.getParents().get(0).getId().equals("ENTRY"))) {
                releaseTask(t);
            }
        }
    }

    public int getOwner() {
        return owner;
    }

    public void setOwner(int owner) {
        this.owner = owner;
    }

    public DAG getDAG() {
        return dag;
    }

    public void setDAG(DAG dag) {
        this.dag = dag;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    /** Check to see if a task has been released */
    public boolean isReleased(Job t) {
        return releasedTasks.contains(t);
    }

    /** Check to see if a task has been completed */
    public boolean isComplete(Task t) {
        return completedTasks.contains(t);
    }

    /** Return true if the workflow is finished */
    public boolean isFinished() {
        // The workflow must be finished if all the tasks that
        // have been released have been completed
        return releasedTasks.size() == completedTasks.size();
    }

    private void releaseTask(Task t) {
    	//Change by maria, only release a task if 
        releasedTasks.add(t);
        queue.add(t);
    }

    /** Mark a task as completed */
    public void completeTask(Task t) {
        // Sanity check
        if (!releasedTasks.contains(t)) {
            throw new RuntimeException("Task has not been released: " + t);
        }

        // Add it to the list of completed tasks
        completedTasks.add(t);

        // Release all ready children
        for (Task c : t.getChildren()) {
			if (!c.getId().equals("EXIT")) {
				if (!releasedTasks.contains(c)) {
					if (completedTasks.containsAll(c.getParents())) {
						releaseTask(c);
					}
				}
			}
		}
	}

    public void recordJobExecution(Job job) {
    	completedJobs.add(job);
    }
    

    /** Return the next ready task */
    public Task nextReadyTask() {
        if (queue.size() <= 0)
            return null;
        return queue.pop();
    }

    /** Return the number of ready tasks */
    public int readyTasks() {
        return queue.size();
    }

	public Job getJob(Task task) {
		for (Job job : completedJobs) {
			if(job.getTask().equals(task)) {
				return job;
			}
		}
		return null;
	}
	
	public double getStartTime() {
		return startTime;
	}
	
	public void setSpareBudget(double dagSpareBudget){
		this.dagSpareBudget = dagSpareBudget;
	}
	
	public double getSpareBudget(){
		return dagSpareBudget;
	}
	
	public Set<Job> getCompletedJobs(){
		return completedJobs;
	}
}
