package cws.core.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cws.core.VM;
import cws.core.WorkflowEvent;
import cws.core.cloudsim.CWSSimEntity;
import cws.core.cloudsim.CWSSimEvent;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.dag.DAGJob;
import cws.core.dag.Task;
import cws.core.exception.UnknownWorkflowEventException;
import cws.core.jobs.Job;
import cws.core.storage.StorageManager;

public class VM2VMTransferManager extends CWSSimEntity implements TransferListener{

	private TransferManager transferManager;
	
	 /** Map of jobs' active input transfers - the ones that progress at any given moment */
    private Map<Job, List<Transfer>> inputTransfers = new HashMap<Job, List<Transfer>>();
    
    /** Map of output transfers finished for a given job */
    private Map<Job, List<Transfer>> outputTransfers = new HashMap<Job, List<Transfer>>();
	
	public VM2VMTransferManager(CloudSimWrapper cloudsim){
		super("VM2VMTransferManager", cloudsim);
		transferManager = new TransferManager(cloudsim);
		transferManager.addListener(this);
	}
	
	public void processEvent(CWSSimEvent ev) {
        switch (ev.getTag()) {
        case WorkflowEvent.VM2VM_BEFORE_TASK_START:
        	Job job = (Job) ev.getData();
            /*for (DAGFile file : job.getTask().getInputFiles()) {
                statistics.addBytesToRead(file.getSize());
            }
            statistics.addTotalFilesToRead(job.getTask().getInputFiles().size());*/
            onBeforeTaskStart(job);
            break;
        case WorkflowEvent.VM2VM_AFTER_TASK_COMPLETED:
        	Job jobAfter = (Job) ev.getData();
            /*for (DAGFile file : jobAfter.getTask().getOutputFiles()) {
                statistics.addBytesToWrite(file.getSize());
            }
            statistics.addTotalFilesToWrite(jobAfter.getTask().getOutputFiles().size());*/
            onAfterTaskCompleted(jobAfter);
            break;
        case WorkflowEvent.TRANSFER_COMPLETE:
        	finishTransfer((Transfer)ev.getData());
        	break;
        default:
            throw new UnknownWorkflowEventException("Unknown event in VM2VMTransferManager: " + ev);
        }
    }
	
	/**
     * 1. If the job has no output files the method finishes immediately.
     * 2. Else it creates transfer for each output file. The transfers are then handled by the event system.
     * 
     * @see StorageManager#onAfterTaskCompleted(Task)
     */
    private void onAfterTaskCompleted(Job job) {
        List<DAGFile> files = job.getTask().getOutputFiles();
       // if (files.size() == 0) {
        //	notifyOutputTransfersCompleted(job);
        //} else {
        	notifyWaitingforOutputTransmission(job);
        //}
        
        /*else {
            startTransfers(files, job, writes, WorkflowEvent.GLOBAL_STORAGE_WRITE_PROGRESS, "write");
            congestedParams.addWrites(files.size());
            updateSpeedCongestion();
        }*/
    }

	/**
     * 1. If the job has no input files the method finishes immediately.
     * 2. Else it creates transfer for each input file. The transfers are then handled by the event system.
     * 
     * @see StorageManager#onBeforeTaskStart(Task)
     */
    private void onBeforeTaskStart(Job job) {
    	List<DAGFile> inputFiles = job.getTask().getInputFiles();
    	if(inputFiles.isEmpty()) {
    		notifyInputTransfersCompleted(job);
    	} else {
    		//Find who has the input files
    		List<Task> parents = job.getTask().getParents();
    		
    		//if its an entry task, retrieve from global storage
    		if(parents.isEmpty()) {
    		getCloudsim().send(getId(), getCloudsim().getEntityId("StorageManager"), 0.0,
                    WorkflowEvent.STORAGE_BEFORE_TASK_START, job);
    		} else {
    		DAGJob dagJob = job.getDAGJob();
    		int numTransfers = 0;
    		List<Transfer> transferList = new ArrayList<Transfer>();
    		for (Task parent : parents) {
				if(dagJob.isComplete(parent)) {
					//Find the VM in which the job ran
					Job fromJob = dagJob.getJob(parent);
					VM vm = fromJob.getVM();
					//Find the input file this parent is responsible for
					for (DAGFile input : inputFiles) {
						for (DAGFile parentOutput : parent.getOutputFiles()) {
							if(input.equals(parentOutput)) {
								DAGFile transfer = input;
								//Transfer the file
								Transfer t = startTransfer(fromJob, job, vm, job.getVM(), transfer);
								transferList.add(t);
								numTransfers++;
								break;
							}
						}
					}
				}
				else {
					//TODO what do we do here??
					getCloudsim().log("Parent task not completed before child taks attempt to run");
				}
			}

            inputTransfers.put(job, transferList);
    		
    		
    		if(numTransfers < inputFiles.size()) {
    			//TODO what do we do here??
				getCloudsim().log("Number of transferred input files doesn't match input file list");
    		}
    	}
    	}
    }

	private Transfer startTransfer(Job fromJob, Job toJob, VM from, VM to, DAGFile transferFile) {
		//TODO may be one link per transfer is not a good idea...
		Link link = new Link(from.getBandwidth(), 5);
		long dataSize = transferFile.getSize();
		if(from.equals(to)) { //transfers within the same VM are not considered
			dataSize = 0;
		}
		Transfer transfer = new Transfer(from.getOutputPort(), to.getInputPort(), link, 
				dataSize, getId(), getCloudsim(), toJob, fromJob);
		String logMsg = String.format("VM 2 VM transfer started. File: %s, size: %s, from vm: %s, to vm: %s",
                 transferFile.getName(), transferFile.getSize(), from.getId(), to.getId());
        getCloudsim().log(logMsg);
        getCloudsim().send(getId(), transferManager.getId(), 0, WorkflowEvent.NEW_TRANSFER, transfer);
        return transfer;
        
	}

	public double getTransferTimeEstimation(Transfer t){
		return 0.0;
	}
	
	/**
     * Cleans up after transfer's finish.
     * @param transfer - the transfer that has finished.
     * @param transfers - the map with active transfers this transfer belongs to (e.g. writes or reads).
     * @param transferType - the type of this transfer, e.g. "write".
     * @return true if this was the last transfer in the job, false otherwise.
     */
    private boolean onTransferFinished(Transfer transfer) {
		String logMsg = String
				.format("VM 2 VM transfer finished. Bytes transferred: %d, duration: %f",
						transfer.getTransferSize(), transfer.getTransferTime());
		getCloudsim().log(logMsg);
     
        List<Transfer> jobTransfers = inputTransfers.get(transfer.getToJob());
        jobTransfers.remove(transfer);
        if (jobTransfers.isEmpty()) {
            inputTransfers.remove(transfer.getToJob());
            return true;
        } else {
            return false;
        }
    }
	
	/**
     * Notifies parent VM that all input transfers have completed and thus the job can be started.
     * 
     * @param job - the job for which all input transfers have completed
     */
    protected void notifyInputTransfersCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.VM2VM_INPUT_TRANSFERS_COMPLETED, job);
    }
    
    /**
     * Notifies parent VM that all output transfers have completed and thus the job can be finished.
     * 
     * @param job - the job for which all output transfers have completed
     */
    protected void notifyOutputTransfersCompleted(Job job) {
        getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.VM2VM_OUTPUT_TRANSFERS_COMPLETED, job);
    }
    

	
	private void notifyWaitingforOutputTransmission(Job job) {
		getCloudsim().send(getId(), job.getVM().getId(), 0, WorkflowEvent.VM2VM_OUTPUT_TRANSFERS_WAITING, job);
	}
	
	@Override
	public void transferStarted(Transfer t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void bandwidthChanged(Transfer t) {
		// TODO Auto-generated method stub
		
	}

	public void finishTransfer(Transfer t) {
		if (onTransferFinished(t)) {
			notifyInputTransfersCompleted(t.getToJob());
        }
		
		//Register the finished transfer as a complete output transfer
		Job fromJob = t.getFromJob();
		int totalOutputs = fromJob.getTask().getOutputFiles().size();
		if(outputTransfers.containsKey(fromJob)) {
			outputTransfers.get(fromJob).add(t);	
		} else {
			List<Transfer> transfers = new ArrayList<Transfer>();
			transfers.add(t);
			outputTransfers.put(fromJob, transfers);
		}
		
		//Check if any of the parent tasks has finished transferring all outputs and notify.
		if(outputTransfers.get(fromJob).size() == totalOutputs) {
			notifyOutputTransfersCompleted(fromJob);
			outputTransfers.remove(fromJob);
		}
		
	}

	@Override
	public void transferFinished(Transfer t) {
		// TODO Auto-generated method stub
		
	}

}
