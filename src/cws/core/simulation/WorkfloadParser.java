package cws.core.simulation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import cws.core.dag.DAG;
import cws.core.dag.DAGParser;

public class WorkfloadParser {

	public static List<DAG> parseWorkload(File workloadFile) {
		List<DAG> dags = new ArrayList<DAG>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(workloadFile));
			int workflow_id = 0;
			String line = null;
			while((line = reader.readLine()) != null) {
				String[] args = line.split(",");
				//A line in the workload file should be: wfName, file, budget, deadline,submitTime
				if(args.length == 5) {
					String wfName = args[0].trim();
					String dagFile = args[1].trim();
					double budget = Double.parseDouble(args[2].trim());
					double deadline = Double.parseDouble(args[3].trim());
					double submitTime = Double.parseDouble(args[4].trim());
					//adjust deadline according to submit time
					deadline = deadline + submitTime;
					
					DAG dag = DAGParser.parseDAG(new File(dagFile));
					dag.setId(new Integer(workflow_id).toString());
					dag.setName(wfName);
					dag.setBudget(budget);
					dag.setDeadline(deadline);//new deadline
					dag.setSubmitTime(submitTime);
					dags.add(dag);
					workflow_id++;
					
				} else {
					System.err.println("Wrong workload file format");
				}
			}
			
			reader.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("SUCCESS READING WORKLOAD");
		
		return dags;
	}
}
