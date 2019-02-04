package cws.core.storage;

import java.util.HashMap;

import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;

/**
 * Various statistics associated with storage managers.
 * @see {@link StorageManager}
 */
public class StorageManagerStatistics {
    /** Total bytes requested to read */
    private long totalBytesToRead;
    private HashMap<Task, Long> taskTotalBytesToRead = new HashMap<Task, Long>();
    /** Total bytes requested to write */
    private long totalBytesToWrite;
    private HashMap<Task, Long> taskTotalBytesToWrite = new HashMap<Task, Long>();
    /** Actual bytes read (may be lower than totalFilesToRead because of cache) */
    private long actualBytesRead;
    private HashMap<Task, Long> taskActualBytesRead = new HashMap<Task, Long>();
    /** Total number of files requested to read */
    private int totalFilesToRead;
    private HashMap<Task, Integer> taskTotalFilesToRead = new HashMap<Task, Integer>();
    /** Total number of files requested to write */
    private int totalFilesToWrite;
    private HashMap<Task, Integer> taskTotalFilesToWrite = new HashMap<Task, Integer>();
    /** Actual number of files read (may be lower than totalFilesToRead because of cache) */
    private int actualFilesRead;
    private HashMap<Task, Integer> taskActualFilesRead = new HashMap<Task, Integer>();

    public long getTotalBytesToRead() {
        return totalBytesToRead;
    }
    
    public long getTotalBytesToRead(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		totalBytesToRead += taskTotalBytesToRead.get(t);
    	}
    	return totalBytesToRead;
    }
    
    public void addBytesToRead(long num) {
        this.totalBytesToRead += num;
    }
    
    public void addBytesToRead(Task task, long num){
    	this.taskTotalBytesToRead.put(task, num);
    }
    
    public long getTotalBytesToWrite() {
        return totalBytesToWrite;
    }

    public long getTotalBytesToWrite(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		totalBytesToWrite += taskTotalBytesToWrite.get(t);
    	}
    	return totalBytesToWrite;
    }
    
    public void addBytesToWrite(long num) {
        this.totalBytesToWrite += num;
    }
    
    public void addBytesToWrite(Task task, long num){
    	this.taskTotalBytesToWrite.put(task, num);
    }
    
    public long getActualBytesRead() {
        return actualBytesRead;
    }
    
    public long getActualBytesRead(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		if (taskActualBytesRead.containsKey(t)){
    			actualBytesRead += taskActualBytesRead.get(t);
    		}
    	}
    	return actualBytesRead;
    }
    
    public void addActualBytesRead(long num) {
        this.actualBytesRead += num;
    }
    
    public void addActualBytesRead(Task task, long num) {
        this.taskActualBytesRead.put(task, num);
    } 
    
    public int getTotalFilesToRead() {
        return totalFilesToRead;
    }

    public long getTotalFilesToRead(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		totalFilesToRead += taskTotalFilesToRead.get(t);
    	}
    	return totalFilesToRead;
    }
    
    public void addTotalFilesToRead(int totalFilesToRead) {
        this.totalFilesToRead += totalFilesToRead;
    }

    public void addTotalFilesToRead(Task task, int totalFilesToRead){
    	this.taskTotalFilesToRead.put(task, totalFilesToRead);
    }
    
    public int getTotalFilesToWrite() {
        return totalFilesToWrite;
    }

    public long getTotalFilesToWrite(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		totalFilesToWrite += taskTotalFilesToWrite.get(t);
    	}
    	return totalFilesToWrite;
    }
    
    public void addTotalFilesToWrite(int totalFilesToWrite) {
        this.totalFilesToWrite += totalFilesToWrite;
    }

    public void addTotalFilesToWrite(Task task, int totalFilesToWrite){
    	this.taskTotalFilesToWrite.put(task, totalFilesToWrite);
    }
    
    public int getActualFilesRead() {
        return actualFilesRead;
    }

    public int getActualFilesRead(DAGJob dagJob){
    	DAG dag = dagJob.getDAG();
    	for (String tid : dag.getTasks()){
    		Task t = dag.getTaskById(tid);
    		if(taskActualFilesRead.containsKey(t)){
    			actualFilesRead += taskActualFilesRead.get(t);
    		}
    	}
    	return actualFilesRead;
    }
    
    public void addActualFilesRead(int actualFilesRead) {
        this.actualFilesRead += actualFilesRead;
    }
    
    public void addActualFilesRead(Task task, int actualFilesRead) {
        this.taskActualFilesRead.put(task, actualFilesRead);
    }
}
