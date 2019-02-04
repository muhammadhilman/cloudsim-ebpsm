package cws.core.simulation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.algorithms.Fastest;
import cws.core.algorithms.Slowest;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.core.VMType;
import cws.core.dag.DAG;
import cws.core.dag.DAGParser;
import cws.core.engine.Environment;
import cws.core.engine.EnvironmentFactory;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.simulation.StorageSimulationParams;

public class WorkfloadGenerator {

	private static List<String> workloads = new ArrayList<String>();
	
	private static int workflowId = 0;
	private static final int TOTAL_WF_INSTANCES = 10;
	
	public static void generateWorkloadFile(File workloadFile, List<VMType> vmTypes, File dagInputDir, StorageSimulationParams simulationParams) {
		try {
			PrintStream fileOut = new PrintStream(new FileOutputStream(workloadFile));
						
			int totalInstances = 0;
			double submitTime = 0.0;
			String wfName = null;
			String wfSize = null;
			
			//build the workload
			while (totalInstances < TOTAL_WF_INSTANCES) {
				
				int wfNum = pickRandomInt(1, 5);
				int wfNumSize = pickRandomInt(1, 3);
				
				if (wfNum == 1){
					wfName = "CYBERSHAKE";
				} else if (wfNum == 2){
					wfName = "GENOME";
				} else if (wfNum == 3){
					wfName = "LIGO";
				} else if (wfNum == 4){
					wfName = "MONTAGE";
				} else if (wfNum == 5){
					wfName = "SIPHT";
				}
				
				if (wfNumSize == 1){
					wfSize = "50";
				} else if (wfNumSize == 2){
					wfSize = "100";
				} else if (wfNum == 3){
					wfSize = "1000";
				}
				
				int wfId = pickRandomInt(0, 19);
				
				workloads.add(wfName + ".n." + wfSize + "." + wfId);
				
				totalInstances++;
			}
			
			for (String workload : workloads){
				
				double minBudget = getSimulatedBudget("slowest", workload, vmTypes, dagInputDir, simulationParams);
				double maxBudget = getSimulatedBudget("fastest", workload, vmTypes, dagInputDir, simulationParams);
			
				//Lets make sure min is smaller than max...
				if(minBudget > maxBudget) {
					double temp = minBudget;
					minBudget = maxBudget;
					maxBudget = temp;
				}
				
				submitTime = submitTime + poissonDistSample();
				
				double budget = Math.round(pickRandomDouble(minBudget, maxBudget));
				double deadline = 0.0;
				
				String dagFile = getDagFileName(workload, dagInputDir);
				String line = String.format("%s, %s, %f, %f, %f", workload, dagFile, budget, deadline, submitTime);
				fileOut.println(line);
				
			}
			
			fileOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static double poissonDistSample() {
		double mean = 5;
	    Random r = new Random();
	    double L = Math.exp(-mean);
	    int k = 0;
	    double p = 1.0;
	    do {
	        p = p * r.nextDouble();
	        k++;
	    } while (p > L);
	    return k - 1;
	}

	private static int pickRandomInt(int min, int max) {
		Random random = new Random();
		return random.nextInt((max - min) + 1) + min;
	}
	
	private static double pickRandomDouble(double min, double max) {
		Random random = new Random();
		return min + (max - min) * random.nextDouble();
	}
	
	private static Double getSimulatedBudget(String algorithmName, String wfName,
			List<VMType> vmTypes, File dagInputDir, StorageSimulationParams simulationParams) {
		CloudSimWrapper cloudsim = new CloudSimWrapper();
		cloudsim.init();
		cloudsim.setLogsEnabled(false);
		
		Environment environment = EnvironmentFactory.createEnvironment(cloudsim, 
				simulationParams, vmTypes, false);

		double alpha = 1.0;
		double maxScaling = 1.0;
		List<DAG> dags = new ArrayList<DAG>();
		String inputFile = getDagFileName(wfName, dagInputDir);
		DAG dag = DAGParser.parseDAG(new File(inputFile));
		dag.setId(new Integer(workflowId).toString());
		dag.setName(wfName);
		System.out.println(String.format("Workflow %d, filename = %s", workflowId, wfName));
		workflowId++;
		dags.add(dag);
		Algorithm algorithm = createAlgorithm(alpha, maxScaling, 
				algorithmName, cloudsim, dags, 
				0.0, 0.0, false, environment);
		algorithm.setEnvironment(environment);
		algorithm.simulate();
		AlgorithmStatistics algorithmStatistics = algorithm.getAlgorithmStatistics();
		
		double budget = algorithmStatistics.getActualCost();
		
		if (budget == 0.0){
			budget = Math.ceil(algorithmStatistics.getActualDagFinishTime()/environment.getCheapestVM().getBillingTimeInSeconds()
					*environment.getCheapestVM().getPriceForBillingUnit());
		}
		return budget;
	}
	
	private static String getDagFileName(String wfName, File dagInputDir) {
		return dagInputDir.getAbsolutePath() + "/" + wfName + ".dag";
	}
	
	private static Algorithm createAlgorithm(double alpha, double maxScaling,
			String algorithmName, CloudSimWrapper cloudsim, List<DAG> dags,
			double budget, double deadline, boolean saveRuntimeData, Environment env) {
		AlgorithmStatistics ensembleStatistics = new AlgorithmStatistics(dags,
				cloudsim, env);

		if("fastest".equals(algorithmName)){
			return new Fastest(budget, deadline, dags, ensembleStatistics, cloudsim, saveRuntimeData);
		} else if ("slowest".equals(algorithmName)){
			return new Slowest(budget, deadline, dags, ensembleStatistics, cloudsim, saveRuntimeData);
		} else {
			throw new IllegalCWSArgumentException("Unknown algorithm: "
					+ algorithmName);
		}
	}
}
