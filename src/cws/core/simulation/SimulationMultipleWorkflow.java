package cws.core.simulation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.cloudbus.cloudsim.Log;
import org.yaml.snakeyaml.Yaml;

import cws.core.algorithms.Algorithm;
import cws.core.algorithms.AlgorithmStatistics;
import cws.core.algorithms.EBPSM;
import cws.core.algorithms.MSLBL;
import cws.core.cloudsim.CloudSimWrapper;
import cws.core.config.GlobalStorageParamsLoader;
import cws.core.core.VMType;
import cws.core.core.VMTypeLoader;
import cws.core.core.VMTypeMipsComparator;
import cws.core.dag.DAG;
import cws.core.dag.DAGJob;
import cws.core.engine.Environment;
import cws.core.engine.EnvironmentFactory;
import cws.core.exception.IllegalCWSArgumentException;
import cws.core.provisioner.VMFactory;
import cws.core.simulation.StorageCacheType;
import cws.core.simulation.StorageSimulationParams;
import cws.core.simulation.StorageType;
import cws.core.storage.StorageManagerStatistics;
import cws.core.storage.global.GlobalStorageParams;

public class SimulationMultipleWorkflow {
	
	private static final String DEFAULT_STORAGE_CACHE = "unlimited";
	private static final String DEFAULT_ENABLE_LOGGING = "true";
	private static final String DEFAULT_IS_STORAGE_AWARE = "true";
	private static final String DEFAULT_SAVE_RUNTIME_DATA = "false";
	private static final String WORKLOAD_FILE = "workload/test.csv";//change this for every workload
	private static final int NUMBER_OF_RUNS = 1;
	private static final String CONFIG_FILE = "configMultiple.yaml";
	private static final String CONFIG_DIR = "config";
	private static final List<String> algs = new ArrayList<String>();
	static {
		algs.add("EBPSM");
		algs.add("MSLBL");
	}
	
	private VMTypeLoader vmTypeLoader;
	private GlobalStorageParamsLoader globalStorageParamsLoader;

	public SimulationMultipleWorkflow() {
		this.vmTypeLoader = new VMTypeLoader();
		this.globalStorageParamsLoader = new GlobalStorageParamsLoader();
	}

	public static void main(String[] args) {

		Options options = buildOptions();
		CommandLine cmd = null;

		String configFilename = CONFIG_FILE;
		String configDirectory = CONFIG_DIR;
		
		try {
			InputStream input = new FileInputStream(new File(configDirectory, configFilename));
			Yaml yaml = new Yaml();
			@SuppressWarnings("unchecked")
			Map<String, Object> config = (Map<String, Object>) yaml.load(input);
			List<String> argsList = new ArrayList<String>();
			Set<String> keys = config.keySet();
			Iterator<String> it = keys.iterator();
			
			while (it.hasNext()) {
				String argsOption = "";
				String key = it.next();
				String property = (String) config.get(key);
				if (property != null) {
					argsOption = "--" + key;
					argsList.add(argsOption);
					argsList.add(property);
				}
			}
			String[] type = new String[argsList.size()];
			args = argsList.toArray(type);
		} catch (FileNotFoundException e) {
			throw new IllegalCWSArgumentException("Cannot load default config file: " + e.getMessage());
		}

		try {
			CommandLineParser parser = new PosixParser();
			System.out.println(args[0]);
			System.out.println(args[1]);
			cmd = parser.parse(options, args);
		} catch (ParseException exp) {
			printUsage(options, exp.getMessage());
		}
		
		SimulationMultipleWorkflow sim = new SimulationMultipleWorkflow();
		File outputfile = new File(cmd.getOptionValue("output-file"));
		sim.run(cmd, outputfile);
		
	}

	private void run(CommandLine args, File outputfile) {

		File workloadFile = new File(WORKLOAD_FILE);
		File inputdir = new File(args.getOptionValue("input-dir"));
		String storageManagerType = args.getOptionValue("storage-manager");

		// Arguments with defaults
		long seed = Long.parseLong(args.getOptionValue("seed", System.currentTimeMillis() + ""));
		String storageCacheType = args.getOptionValue("storage-cache", DEFAULT_STORAGE_CACHE);
		boolean enableLogging = Boolean.valueOf(args.getOptionValue("enable-logging", DEFAULT_ENABLE_LOGGING));
		boolean isStorageAware = Boolean.valueOf(args.getOptionValue("storage-aware", DEFAULT_IS_STORAGE_AWARE));
		boolean saveRuntimeData = Boolean.valueOf(args.getOptionValue("save-runtime-data", DEFAULT_SAVE_RUNTIME_DATA));

		List<VMType> vmTypes = vmTypeLoader.determineVMType(args);
		for (VMType vmType : vmTypes) {
			logVMType(vmType);
		}
		
		double avgPerformanceVar = Double.parseDouble(args.getOptionValue("average-performance-variation", "0.0"));

		CloudSimWrapper cloudsim = new CloudSimWrapper();
		cloudsim.init();
		cloudsim.setLogsEnabled(enableLogging);
		Log.disable(); // We do not need Cloudsim's logs. We have our own.

		StorageSimulationParams simulationParams = new StorageSimulationParams();

		if (storageCacheType.equals("fifo")) {
			simulationParams.setStorageCacheType(StorageCacheType.FIFO);
		} else if (storageCacheType.equals("unlimited")) {
			simulationParams.setStorageCacheType(StorageCacheType.UNLIMITED);
		} else if (storageCacheType.equals("void")) {
			simulationParams.setStorageCacheType(StorageCacheType.VOID);
		} else {
			throw new IllegalCWSArgumentException("Wrong storage-cache:" + storageCacheType);
		}

		if (storageManagerType.equals("global")) {
			GlobalStorageParams globalStorageParams = globalStorageParamsLoader.determineGlobalStorageParams(args);
			logGlobalStorageParams(globalStorageParams);
			simulationParams.setStorageParams(globalStorageParams);
			simulationParams.setStorageType(StorageType.GLOBAL);
		} else if (storageManagerType.equals("void")) {
			simulationParams.setStorageType(StorageType.VOID);
		} else {
			throw new IllegalCWSArgumentException("Wrong storage-manager:" + storageCacheType);
		}

		// Echo the simulation parameters
		System.out.printf("inputdir = %s\n", inputdir);
		System.out.printf("outputfile = %s\n", outputfile);
		System.out.printf("seed = %d\n", seed);
		System.out.printf("storageManagerType = %s\n", storageManagerType);
		System.out.printf("storageCache = %s\n", storageCacheType);
		System.out.printf("enableLogging = %b\n", enableLogging);
		System.out.printf("isStorageAware = %b\n", isStorageAware);

		Environment environment = EnvironmentFactory.createEnvironment(cloudsim, simulationParams, vmTypes,
				isStorageAware);

		PrintStream fileOutDetailed = null;
		PrintStream fileOutGeneral = null;
		try {
			fileOutDetailed = new PrintStream(new FileOutputStream(outputfile + "_detailed.csv"));
			fileOutDetailed.print("Workflow, Algorithm, Tasks, Submit Time, "
					+ "Deadline, Finish Time, Makespan, Deadline Met, "
					+ "Budget, Cost, Spare Budget, Budget Met, VMs Used, ");
			Collections.sort(environment.getVmTypes(), new VMTypeMipsComparator());
			for (VMType type : environment.getVmTypes()){
				fileOutDetailed.print("Num " + type.getName() + ", ");
				fileOutDetailed.print("Prov. delay " + type.getName() + ", ");
				fileOutDetailed.print("Deprov. delay " + type.getName() + ", ");
			}
			fileOutDetailed.println();
			
			fileOutGeneral = new PrintStream(new FileOutputStream(outputfile + "_general.csv"));
			fileOutGeneral.print("Algorithm, Num. Workflows, Total Tasks, "
					+ "Total Makespan, Deadlines Met, "
					+ "Total Cost, Budgets Met, "
					+ "Total VMs, Avg. VM Util., Overall Util., "
					+ "Simulation Time, Storage Manager, "
					+ "Cache Manager, Runtime Variation, "
					+ "Failure Rate, Performance Variation, ");
			Collections.sort(environment.getVmTypes(), new VMTypeMipsComparator());
			for (VMType type : environment.getVmTypes()) {
				fileOutGeneral.print("Num " + type.getName() + ", ");
				fileOutGeneral.print("Prov. delay " + type.getName() + ", ");
				fileOutGeneral.print("Deprov. delay " + type.getName() + ", ");
			}
			fileOutGeneral.println();
			
			VMFactory.readCliOptions(args, seed);
			System.out.println();
			
			System.out.println("BEGIN GENERATING WORKLOAD");
			
			WorkfloadGenerator.generateWorkloadFile(workloadFile, vmTypes, inputdir, simulationParams);
			
			System.out.println("SUCCESS GENERATING WORKLOAD");
			
			List<DAG> dags= WorkfloadParser.parseWorkload(workloadFile);
			
			for (String algorithm : algs){
				
				runExperiment(cloudsim, algorithm, fileOutDetailed, fileOutGeneral, 
						outputfile, dags, enableLogging, storageManagerType,
						storageCacheType, simulationParams, vmTypes, isStorageAware, 
						avgPerformanceVar, saveRuntimeData);
			}
			
			fileOutDetailed.flush();
			fileOutGeneral.flush();
			
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} finally {
			IOUtils.closeQuietly(fileOutDetailed);
			IOUtils.closeQuietly(fileOutGeneral);
		}
	
	}

	private void runExperiment(CloudSimWrapper cloudsim, String algorithmName, 
			PrintStream fileOutDetailed,PrintStream fileOutGeneral,
			File outputfile, List<DAG> dags, boolean enableLogging, 
			String storageManagerType, String storageCacheType, 
			StorageSimulationParams simulationParams, List<VMType> vmTypes, 
			boolean isStorageAware, double avgPerformanceVar, boolean saveRuntimeData) {
		
		AlgorithmStatistics algorithmStatistics = null;
		StorageManagerStatistics stats = null;
		
		int tasks = 0;
		double makespan = 0.0;
		double simulationTime = 0.0;
		double actualCost = 0.0;
		int numVms = 0;
		double avgVmUtil = 0.0;
		double systemUtil = 0.0;
		int numWorkflows = 0;
		
		//For each dag
		double dagMakespan = 0.0;
		double dagFinishTime = 0.0;
		double dagDeadline = 0.0;
		double dagCost = 0.0;
		double dagBudget = 0.0;
		double dagUnspentBudget = 0.0;
		int dagVmsUsed = 0;
		boolean metDeadline = false;
		boolean metBudget = false;
		int dagTasks = 0;
		double numDeadlinesMet = 0;
		double numBudgetsMet = 0;
		
		int successfulRun = 0;
				
		for (int i = 0; i < NUMBER_OF_RUNS; i++) {
			try {
				if (enableLogging) {
					cloudsim = new CloudSimWrapper(getLogOutputStream(
							algorithmName,
							outputfile));
					//cloudsim = new CloudSimWrapper(System.out);
				} else {
					cloudsim = new CloudSimWrapper();
				}
				cloudsim.init();
				cloudsim.setLogsEnabled(enableLogging);

				Environment environment = EnvironmentFactory.createEnvironment(
						cloudsim, simulationParams, vmTypes, isStorageAware);

				double alpha = 1.0;
				double maxScaling = 1.0;
				double budget = 0.0; //not used
				double deadline = 0.0; //overriden by dag.getDeadline
				//just to be compatible with algorithms that expect the deadline 
				//be part of the engine
				Algorithm algorithm = createAlgorithm(alpha, maxScaling,
						algorithmName, cloudsim, dags, budget, deadline, saveRuntimeData, environment);

				algorithm.setEnvironment(environment);
				
				algorithm.simulate();

				algorithmStatistics = algorithm.getAlgorithmStatistics();
				stats = environment.getStorageManagerStatistics();

				if (!algorithmStatistics.getFinishedDags().isEmpty()) {
					successfulRun++;
					Set<DAGJob> completedDags = algorithmStatistics.getDagJobCompletionTimes().keySet();
					
					for (DAGJob dagJob : completedDags) {
						
						//must be changed after experiment
						dagFinishTime = algorithmStatistics.getDagJobCompletionTimes().get(dagJob);
						dagMakespan = algorithmStatistics.getDagJobCompletionTimes().get(dagJob) - dagJob.getStartTime();
						dagDeadline = dagJob.getDAG().getDeadline();
						dagCost = algorithmStatistics.getDagJobCosts().get(dagJob);
						dagBudget = dagJob.getDAG().getBudget();
						dagUnspentBudget = dagBudget - dagCost;
						
						
						dagVmsUsed = algorithmStatistics.getDagVmsUsed().get(dagJob);
						metDeadline = dagFinishTime <= dagDeadline ? true : false;
						metBudget = dagCost <= dagBudget ? true :  false;
						if(metDeadline) {
							numDeadlinesMet++;
						}
						if(metBudget) {
							numBudgetsMet++;
						}
						dagTasks = dagJob.getDAG().getTasks().length;

						fileOutDetailed.printf("%s, %s, %d, %f, %f, %f, %f, %b, %f, %f, %f, %b, ",
								dagJob.getDAG().getName(), algorithmName,
								dagTasks, dagJob.getDAG().getSubmitTime(),
								dagDeadline, dagFinishTime, dagMakespan,
								metDeadline, dagBudget, dagCost, dagUnspentBudget, metBudget);

						fileOutDetailed.printf("%d, ", dagVmsUsed);
						
						SortedMap<VMType, Integer> numVmsPerType = algorithmStatistics.getDagNumVmsPerType(dagJob);
						
						for (VMType type : numVmsPerType.keySet()) {
							int num = numVmsPerType.get(type);
							fileOutDetailed.printf("%d, ", num);
							fileOutDetailed.printf("%f, ", type.getProvisioningDelay().sample());
							fileOutDetailed.printf("%f, ", type.getDeprovisioningDelay().sample());
						}
						
						fileOutDetailed.println();
					}
					
					//General file
					//Common to all dags
					simulationTime = cloudsim.getSimulationWallTime() / 1.0e9;
					actualCost = algorithmStatistics.getActualCost();
					makespan = algorithmStatistics.getActualDagFinishTime();
					numVms = algorithmStatistics.getNumVms();
					avgVmUtil = algorithmStatistics.getAverageVMUtilization();
					systemUtil = algorithmStatistics.getOverallSystemUtilization();
					tasks = algorithmStatistics.getFinishedTasks();
					numWorkflows = algorithmStatistics.getFinishedDags().size();
					
					fileOutGeneral.printf("%s, %d, %d, %f, %f, ", algorithmName,
							numWorkflows, tasks, makespan, numDeadlinesMet);

					fileOutGeneral.printf("%f, %f, ", actualCost, numBudgetsMet);
					
					fileOutGeneral.printf("%d, %f, %f, %f, ", numVms, avgVmUtil, systemUtil, simulationTime);
					
					fileOutGeneral.printf("%s, %s, %f, %f, %f, ", storageManagerType,
							storageCacheType, VMFactory.getRuntimeVariance(),
							VMFactory.getFailureRate(), avgPerformanceVar); 

					SortedMap<VMType, Integer> numVmsPerType = algorithmStatistics.getNumVmsPerType();
					for (VMType type : numVmsPerType.keySet()) {
						int num = numVmsPerType.get(type);
						fileOutGeneral.printf("%d, ", num);
						fileOutGeneral.printf("%f, ", type.getProvisioningDelay().sample());
						fileOutGeneral.printf("%f, ", type.getDeprovisioningDelay().sample());
					}
					
					fileOutGeneral.println();
				}

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static Options buildOptions() {
		Options options = new Options();

		Option seed = new Option("s", "seed", true, "Random number generator seed, defaults to current time in milis");
		seed.setArgName("SEED");
		options.addOption(seed);

		Option inputdir = new Option("id", "input-dir", true, "(required) Input dir");
		inputdir.setRequired(true);
		inputdir.setArgName("DIR");
		options.addOption(inputdir);

		Option outputfile = new Option("of", "output-file", true, "(required) Output file");
		outputfile.setRequired(true);
		outputfile.setArgName("FILE");
		options.addOption(outputfile);

		Option storageCache = new Option("sc", "storage-cache", true,
				"Storage cache, defaults to " + DEFAULT_STORAGE_CACHE);
		storageCache.setArgName("CACHE");
		options.addOption(storageCache);

		Option storageManager = new Option("sm", "storage-manager", true, "(required) Storage manager ");
		storageManager.setRequired(true);
		storageManager.setArgName("MRG");
		options.addOption(storageManager);

		Option enableLogging = new Option("el", "enable-logging", true,
				"Whether to enable logging, defaults to " + DEFAULT_ENABLE_LOGGING);
		enableLogging.setArgName("BOOL");
		options.addOption(enableLogging);

		Option isStorageAware = new Option("sa", "storage-aware", true,
				"Whether the algorithms should be storage aware, defaults to " + DEFAULT_IS_STORAGE_AWARE);
		isStorageAware.setArgName("BOOL");
		options.addOption(isStorageAware);

		Option saveRuntimeData = new Option("srd", "save-runtime-data", true,
				"Whether runtime data is stored in historical database, defaults to " + DEFAULT_SAVE_RUNTIME_DATA);
		saveRuntimeData.setArgName("BOOL");
		options.addOption(saveRuntimeData);

		VMFactory.buildCliOptions(options);

		VMTypeLoader.buildCliOptions(options);
		GlobalStorageParamsLoader.buildCliOptions(options);

		return options;
	}
	
	private static void printUsage(Options options, String reason) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(120);
		formatter.printHelp(SimulationMultipleWorkflow.class.getName(), "", options, reason);
		System.exit(1);
	}
	
	private void logVMType(VMType vmType) {
		System.out.printf("VM mips = %d\n", vmType.getMips());
		System.out.printf("VM cores = %d\n", vmType.getCores());
		System.out.printf("VM memory = %f\n", vmType.getMemory());
		System.out.printf("VM name = %s\n", vmType.getName());
		System.out.printf("VM price = %f\n", vmType.getPriceForBillingUnit());
		System.out.printf("VM unit = %f\n", vmType.getBillingTimeInSeconds());
		System.out.printf("VM cache = %d\n", vmType.getCacheSize());
		System.out.printf("VM provisioningDelay = %s\n",
				vmType.getProvisioningDelay());
		System.out.printf("VM deprovisioningDelay = %s\n",
				vmType.getDeprovisioningDelay());
	}
	
	private void logGlobalStorageParams(GlobalStorageParams globalStorageParams) {
		System.out.printf("GS read speed = %f\n",
				globalStorageParams.getReadSpeed());
		System.out.printf("GS write speed = %f\n",
				globalStorageParams.getWriteSpeed());
		System.out
				.printf("GS latency = %f\n", globalStorageParams.getLatency());
		System.out.printf("GS chunk transfer time = %f\n",
				globalStorageParams.getChunkTransferTime());
		System.out.printf("GS replicas number = %d\n",
				globalStorageParams.getNumReplicas());
	}
	
	private OutputStream getLogOutputStream(String algorithm, File outputfile) {
		String name = String.format("%s.%s.log",
				outputfile.getAbsolutePath(), algorithm);
		try {
			return new FileOutputStream(new File(name));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	protected Algorithm createAlgorithm(double alpha, double maxScaling,
			String algorithmName, CloudSimWrapper cloudsim, List<DAG> dags,
			double budget, double deadline, boolean saveRuntimeData, Environment env) {
		AlgorithmStatistics ensembleStatistics = new AlgorithmStatistics(dags,
				cloudsim, env);

		if ("EBPSM".equals(algorithmName)) {
			return new EBPSM(budget, deadline, dags, ensembleStatistics, cloudsim, saveRuntimeData);
		} else if ("MSLBL".equals(algorithmName)){
			return new MSLBL(budget, deadline, dags, ensembleStatistics, cloudsim, saveRuntimeData);
		} else {
			throw new IllegalCWSArgumentException("Unknown algorithm: "
					+ algorithmName);
		}
	}
}
