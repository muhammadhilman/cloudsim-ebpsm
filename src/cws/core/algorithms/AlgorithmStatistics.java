package cws.core.algorithms;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import cws.core.VM;
import cws.core.VMListener;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.core.VMTypeMipsComparator;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.DAGJobListener;
import cws.core.dag.Task;
import cws.core.engine.Environment;
import cws.core.jobs.Job;
import cws.core.jobs.Job.Result;
import cws.core.jobs.JobListener;

public class AlgorithmStatistics extends CWSSimEntity implements DAGJobListener, VMListener, JobListener {
    private List<DAG> allDags;
    private Environment env;
    
    public AlgorithmStatistics(List<DAG> allDags, CloudSimWrapper cloudsim) {
        super("AlgorithmStatistics", cloudsim);
        this.allDags = allDags;
        
        List<VMType> vmTypes = null;
        vmTypes = env.getVmTypes();
    	for (VMType vmType : vmTypes){
    		numVmsPerType.put(vmType, 0);
    	}
    }
    
    public AlgorithmStatistics(List<DAG> allDags, CloudSimWrapper cloudsim, Environment env) {
        super("AlgorithmStatistics", cloudsim);
        this.allDags = allDags;
        this.env = env;
        
        List<VMType> vmTypes = null;
        vmTypes = env.getVmTypes();
    	for (VMType vmType : vmTypes){
    		numVmsPerType.put(vmType, 0);
    	}
    }

    private double actualJobFinishTime = 0.0;
    private double actualVmFinishTime = 0.0;
    private double actualDagFinishTime = 0.0;
    private List<DAG> finishedDags = new ArrayList<DAG>();
    private double cost = 0.0;
    private int numVms = 0;
    private double totalJobRuntime = 0.0;
    private double totalVMRuntime = 0.0;
    private double sumVMUtilization = 0.0;
    private double totalBillingPeriodsConsumed = 0;
    
    private double dagJobCompletionTime = 0.0;
    private double dagJobCost = 0.0;
    private double eachVmCost = 0.0;
    private double eachVmRuntime = 0.0;
    private double jobRuntime = 0.0;
    
    private HashMap<DAGJob, Double> dagJobCompletionTimes = new HashMap<DAGJob, Double>();
    private HashMap<DAGJob, Double> dagJobCosts = new HashMap<DAGJob, Double>();
    private HashMap<VM, Double> eachVmCosts = new HashMap<VM, Double>();
    private HashMap<VM, Double> eachVmRuntimes = new HashMap<VM, Double>();
    private HashMap<Task, Double> jobRuntimes = new HashMap<Task, Double>();
    private HashMap<Task, VM> jobVms = new HashMap<Task, VM>();
    private HashMap<VM, Double> vmRuntimes = new HashMap<VM, Double>();
    private SortedMap<VMType, Integer> numVmsPerType = new TreeMap<VMType, Integer>(new VMTypeMipsComparator());
    private HashMap<DAGJob, SortedMap<VMType, Integer>> dagNumVmsPerTypeOuter = new HashMap<DAGJob, SortedMap<VMType, Integer>>();
    private SortedMap<VMType, Integer> dagNumVmsPerTypeInner = new TreeMap<VMType, Integer>(new VMTypeMipsComparator());
    private HashMap<DAGJob, Integer> dagVmsUsed = new HashMap<DAGJob, Integer>();
    
    @Override
    public void shutdownEntity() {
    	getCloudsim().log("Actual cost: " + this.getActualCost());
        getCloudsim().log("Last DAG finished at: " + this.getActualDagFinishTime());
        getCloudsim().log("Last time VM terminated at: " + this.getActualVMFinishTime());
        getCloudsim().log("Last time Job terminated at: " + this.getActualJobFinishTime());
    }

    public List<Integer> getFinishedDAGPriorities() {
        List<Integer> priorities = new LinkedList<Integer>();
        for (DAG dag : getFinishedDags()) {
            int index = allDags.indexOf(dag);
            int priority = index;
            priorities.add(priority);
        }
        return priorities;
    }

    public String getFinishedDAGPriorityString() {
        StringBuilder b = new StringBuilder("[");
        boolean first = true;
        for (int priority : getFinishedDAGPriorities()) {
            if (!first) {
                b.append(", ");
            }
            b.append(priority);
            first = false;
        }
        b.append("]");
        return b.toString();
    }

    /** score = sum[ 1 / 2^priority ] */
    public double getExponentialScore() {
        BigDecimal one = BigDecimal.ONE;
        BigDecimal two = new BigDecimal(2.0);

        BigDecimal score = new BigDecimal(0.0);
        for (int priority : getFinishedDAGPriorities()) {
            BigDecimal divisor = two.pow(priority);
            BigDecimal increment = one.divide(divisor);
            score = score.add(increment);
        }
        return score.doubleValue();
    }
  
    /** score = sum[ 1 / priority ] */
    public double getLinearScore() {
        double score = 0.0;
        for (int priority : getFinishedDAGPriorities()) {
            score += 1.0 / (priority + 1);
        }
        return score;
    }

    public String getScoreBitString() {
        HashSet<Integer> priorities = new HashSet<Integer>(getFinishedDAGPriorities());

        int ensembleSize = allDags.size();

        StringBuilder b = new StringBuilder();

        for (int p = 0; p < ensembleSize; p++) {
            if (priorities.contains(p)) {
                b.append("1");
            } else {
                b.append("0");
            }
        }
        return b.toString();
    }

    public double getActualCost() {
        return cost;
    };

    public double getActualDagFinishTime() {
        return actualDagFinishTime;
    };

    public double getActualJobFinishTime() {
        return actualJobFinishTime;
    };

    public double getActualVMFinishTime() {
        return actualVmFinishTime;
    };

    public List<DAG> getFinishedDags() {
        return finishedDags;
    };
    
    public int getFinishedTasks(){
    	int length = 0;
    	for (DAG dag : finishedDags){
    		length += dag.getTasks().length;
    	}
    	return length;
    }
    
    @Override
    public void jobFinished(Job job) {
    	//actual job finish time
        if (job.getResult() == Result.SUCCESS) {
            actualJobFinishTime = Math.max(actualJobFinishTime, job.getFinishTime());
        }
        
        //job runtime (whether success or failed)
        jobRuntime = job.getDuration();
        
        //total job runtime
        totalJobRuntime += job.getDuration();
        
        //storing runtime for each task (if the task is re-scheduled runtime is sum of all the job runtime)
        if (jobRuntimes.containsKey(job.getTask())){
        	jobRuntimes.put(job.getTask(), jobRuntimes.get(job.getTask()) + jobRuntime);
        } else {
        	jobRuntimes.put(job.getTask(), jobRuntime);
        }
        
        //storing VM for each task
        jobVms.put(job.getTask(), job.getVM());
    }

    @Override
    public void vmLaunched(VM vm) {
    	//total number of VMs
    	numVms++;
    	
    	//number of VMs for each type
    	if(numVmsPerType.containsKey(vm.getVmType())){
    		numVmsPerType.put(vm.getVmType(), numVmsPerType.get(vm.getVmType()) + 1);
    	}
    }

    public int getNumVms() {
    	return numVms;
    }
    
    public SortedMap<VMType, Integer> getNumVmsPerType(){
    	return numVmsPerType;
    }
    
    public double getAverageVMUtilization() {
    	return sumVMUtilization/numVms;
    }
    
    public double getOverallSystemUtilization() {
    	return totalJobRuntime/totalVMRuntime;
    }
    
    public double getRealCost() {
    	return totalJobRuntime/totalBillingPeriodsConsumed;
    }
    
    @Override
    public void vmTerminated(VM vm) {
        //total vm cost
    	cost += vm.getCost();
        
    	//total VMs runtime
        totalVMRuntime += vm.getRuntime();
    	
    	//vm cost
    	eachVmCost = vm.getCost();
        eachVmCosts.put(vm, eachVmCost);
        
        //vm runtime
        eachVmRuntime = vm.getRuntime();
        eachVmRuntimes.put(vm, eachVmRuntime);
        
        //actual vm finish time
        actualVmFinishTime = Math.max(actualVmFinishTime, getCloudsim().clock());
        
        //sum of VMs utilization
        sumVMUtilization += vm.getUtilization();
        
        double billingPeriods = cost/vm.getVmType().getPriceForBillingUnit();
        totalBillingPeriodsConsumed += billingPeriods * vm.getVmType().getBillingTimeInSeconds();
    }

    @Override
    public void dagStarted(DAGJob dagJob) {
    }

    @Override
    public void dagFinished(DAGJob dagJob) {
        actualDagFinishTime = Math.max(actualDagFinishTime, getCloudsim().clock());
        finishedDags.add(dagJob.getDAG());
        
        List<VMType> vmTypes = null;
        vmTypes = env.getVmTypes();
    	for (VMType vmType : vmTypes){
    		dagNumVmsPerTypeInner.put(vmType, 0);
    	}
        
    	
    	//dag finish time
        dagJobCompletionTime = getCloudsim().clock();
        dagJobCompletionTimes.put(dagJob, dagJobCompletionTime);
        
        vmRuntimes.clear();
        dagJobCost = 0.0;
        
        //storing task runtime on each VM
        for (String tid : dagJob.getDAG().getTasks()){
        	Task t = dagJob.getDAG().getTaskById(tid);
        	double taskRuntime = jobRuntimes.get(t);
        	VM vm = jobVms.get(t);
        	if(vmRuntimes.containsKey(vm)){
        		vmRuntimes.put(vm, vmRuntimes.get(vm) + taskRuntime);
        	} else {
        		vmRuntimes.put(vm, taskRuntime);
        	}
        }
        
        //calculating cost for a dag
        for (VM vm : vmRuntimes.keySet()){
        	double vmRuntime = vmRuntimes.get(vm);
        	double vmPrice = vm.getVmType().getPriceForBillingUnit();
        	double vmBillingUnit = vm.getVmType().getBillingTimeInSeconds();
        	
        	dagJobCost += Math.ceil(vmRuntime/vmBillingUnit)*vmPrice;
        	
        	if (dagNumVmsPerTypeInner.containsKey(vm.getVmType())){
        		dagNumVmsPerTypeInner.put(vm.getVmType(), dagNumVmsPerTypeInner.get(vm.getVmType()) + 1);
        	}
        }
        
        dagJobCosts.put(dagJob, dagJobCost);
        dagVmsUsed.put(dagJob, vmRuntimes.keySet().size());
        dagNumVmsPerTypeOuter.put(dagJob, dagNumVmsPerTypeInner);
        
    }

    public HashMap<DAGJob, Double> getDagJobCompletionTimes(){
    	return dagJobCompletionTimes;
    }
    
    public HashMap<DAGJob, Double> getDagJobCosts(){
    	return dagJobCosts;
    }
    
    public HashMap<DAGJob, Integer> getDagVmsUsed(){
    	return dagVmsUsed;
    }
    
    public SortedMap<VMType, Integer> getDagNumVmsPerType(DAGJob dagJob){
    	return dagNumVmsPerTypeOuter.get(dagJob);
    }
    
    @Override
    public void jobReleased(Job job) {
    }

    @Override
    public void jobSubmitted(Job job) {
    }

    @Override
    public void jobStarted(Job job) {
    }
}
