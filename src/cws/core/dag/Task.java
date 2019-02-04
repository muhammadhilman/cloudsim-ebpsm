package cws.core.dag;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gideon Juve <juve@usc.edu>
 */
public class Task {
    /** Globally uniqe task id */
    private String id = null;

    /** Transformation string taken from some daxes. Not really important and used only for logging. */
    private String transformation = null;

    /** Number of MIPS needed to compute this task */
    private double size = 0.0;

    /** Task's parents - the tasks that produce inputFiles */
    private List<Task> parents = new ArrayList<Task>(2);

    /** Task's children - the tasks which this Task produce files for */
    private List<Task> children = new ArrayList<Task>(5);

    /** Task's input files */
    private List<DAGFile> inputFiles = new ArrayList<DAGFile>();

    /** Task's output files */
    private List<DAGFile> outputFiles = new ArrayList<DAGFile>();
    
    private double memoryRequirement;
    
    private String type;
    
    private boolean transferOutputs;
    
    private double deadline;//task deadline
    
    private double budget;//task budget
    
    private double earliestStartTime;//task EST
    
    private double earliestFinishTime;//task EFT
    
    private double rank;//task rank

    public Task(String id, String transformation, double size, double memoryRequirement) {
        this.id = id;
        this.transformation = transformation;
        this.type = transformation;
        this.size = size;
        this.memoryRequirement = memoryRequirement;
        transferOutputs = true;
        deadline = 0.0;
        budget = 0.0;
        earliestStartTime = 0.0;
        earliestFinishTime = 0.0;
        rank = 0.0;
    }

    /**
     * IT IS IMPORTANT THAT THESE ARE NOT IMPLEMENTED
     * 
     * Using the default implementation allows us to put Tasks from
     * different DAGs that have the same task ID into a single HashMap
     * or HashSet. That way we don't have to maintain a reference from
     * the Task to the DAG that owns it--we can mix tasks from different
     * DAGs in the same data structure.
     * 
     * <pre>
     * public int hashCode() {
     *     return id.hashCode();
     * }
     * 
     * public boolean equals(Object o) {
     *     if (!(o instanceof Task)) {
     *         return false;
     *     }
     *     Task t = (Task) o;
     *     return this.id.equals(t.id);
     * }
     * </pre>
     */

    @Override
    public String toString() {
        return "<task id=" + getId() + ">";
    }

    public void scaleSize(double scalingFactor) {
        size *= scalingFactor;
    }

    public double getSize() {
        return size;
    }

    public String getTransformation() {
        return transformation;
    }

    public String getId() {
        return id;
    }

    public List<Task> getParents() {
        return parents;
    }

    public List<Task> getChildren() {
        return children;
    }

    public List<DAGFile> getInputFiles() {
        return inputFiles;
    }

    public void addInputFiles(List<DAGFile> inputs) {
        this.inputFiles.addAll(inputs);
    }

    public List<DAGFile> getOutputFiles() {
        return outputFiles;
    }

    public void addOutputFiles(List<DAGFile> outputs) {
        this.outputFiles.addAll(outputs);
    }
    
    public double getMemoryRequirement() {
    	return memoryRequirement; 
    }
    
    public void setTransferOutputs(boolean transferOutputs) {
    	this.transferOutputs = transferOutputs;
    }

	public boolean getTransferOutputs() {
		return transferOutputs;
	}
	
	public String getType() {
		return type;
	}
	
	public void setBudget(double budget){
		this.budget = budget;
	}
	
	public double getBudget(){
		return budget;
	}
	
	public void setDeadline(double deadline){
		this.deadline = deadline;
	}
	
	public double getDeadline(){
		return deadline;
	}
	
	public void setEarliestStartTime(double earliestStartTime){
		this.earliestStartTime = earliestStartTime;
	}
	
	public double getEarliestStartTime(){
		return earliestStartTime;
	}
	
	public void setEarliestFinishTime(double earliestFinishTime){
		this.earliestFinishTime = earliestFinishTime;
	}
	
	public double getEarliestFinishTime(){
		return earliestFinishTime;
	}	
	
	public void setRank(double rank){
		this.rank = rank;
	}
	
	public double getRank(){
		return rank;
	}
}
