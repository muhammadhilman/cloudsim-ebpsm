package cws.core.transfer;

import cws.core.cloudsim.CloudSimWrapper;
import cws.core.dag.DAGFile;
import cws.core.jobs.Job;

public class DAGFileTransfer extends Transfer {
	
	private boolean isInputTransfer;
	private DAGFile dagFile;
	private Job job;

	public DAGFileTransfer(Port source, Port destination, Link link,
			long dataSize, int owner, CloudSimWrapper cloudsim, boolean isInputTransfer, DAGFile dagFile, Job job) {
		super(source, destination, link, dataSize, owner, cloudsim, null, null);
		this.isInputTransfer = isInputTransfer;
		this.dagFile = dagFile;
		this.job = job;
	}

	public boolean isInputTransfer() {
		return isInputTransfer;
	}

	public DAGFile getDagFile() {
		return dagFile;
	}
	
	public Job getJob() {
		return job;
	}

}
