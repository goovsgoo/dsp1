package dsp1_v1;


import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;


import dsp1_v1.AWSHandler.QueueType;

public class Manager {

	private AWSHandler 	aws;
	private  boolean isTerminate = false; 
	private int tweetsPerWorker;
	private Map<String, Integer> expectedResultsNum = new HashMap<String, Integer>();	
	private ExecutorService requestExequtor;
	
	public Manager() {
		aws = new AWSHandler();
		requestExequtor = Executors.newFixedThreadPool(10);
	}
	
	private void pullMessageAndDeliverTaskNewThread() {
		 //System.out.println("::MANAGER:: Try to get file from local");

	        Message message = aws.pullMessageFromSQS(QueueType.LocalToManager);
	        if(message == null){
	            return;
	        }
	        else if (isTerminateMessage(message)) {
	            System.out.println("::MANAGER:: Got [Terminate] message from local");
	            isTerminate = true;
	            terminateManager();
	            return;
	        } 
	        else {
	        	System.out.println("::MANAGER:: found a task to do");
	        	
	        	DeliverTask deliverTask = new DeliverTask(message ,aws , expectedResultsNum , tweetsPerWorker);
	    		try {
	    			//expectedResultsNum =  (Map<String, Integer>) requestExequtor.submit(deliverTask);  /// Or solve this pls!!
	    			 Future<Object> future = requestExequtor.submit(deliverTask);
	    			 expectedResultsNum = (Map<String, Integer>) future.get();
	    			System.out.println("::MANAGER:: Thread DeliverTask start!");
	    		} 
	    		catch (Exception e) {
	    			System.out.println("::MANAGER::  Thread DeliverTask FAILED error: " + e.getMessage());
	    		}
	        }
	}
	/**
	 * function for "main" or "run" - loop and ck and sum for new msg from the workers. when all task received reduce and upload to S3
	 * @param
	 * @return
	 */
	private void checkIfTaskFinished() {				//reduce(taskID);				//reduce(taskID);
		Message message = null;			
		//Map<String, Integer> receivedResultsNum = new HashMap<String, Integer>();
		
		if ((message = aws.pullMessageFromSQS(QueueType.WorkerToManager)) != null) {
			System.out.println("::Manager:: message: " + message.getBody());
			String taskID = message.getMessageAttributes().get("taskID").getStringValue();
			File file = addHtmlTagMessageToFile(taskID,message);
			expectedResultsNum.put(taskID, expectedResultsNum.get(taskID).intValue()-1);
			aws.deleteMessageFromSQS(message, QueueType.WorkerToManager);
			
			if (expectedResultsNum.get(taskID).intValue() == 0) {
				System.out.println("::Manager:: upload reduce file To S3 taskID: " + taskID);
				aws.uploadFileToS3(file, taskID);
			}
		}
		//System.out.println("::Manager:: Queue WorkerToManager is empty");
	}
	
	/**
	 * create or open file that contain all the HTML Tweet from the same local
	 * @param String taskID , message with HTML tag
	 * @return file with the massage inside
	 */
	private File addHtmlTagMessageToFile(String taskID , Message message)
    {	
		File file = new File ("reduce_" + taskID + ".txt");
		FileWriter fileWriter;
		PrintWriter writer;
        try
        {
        	fileWriter = new FileWriter(file, true);	//append write mode
        	writer = new PrintWriter(fileWriter); 
            
            writer.println(message.getBody());
            
            writer.close();
            return file;
        }
        catch (Exception e)
        {
            System.out.println("::Manager:: HTML Tweet file FAILED : "+e.getMessage());
        }
        return null;
    }
	
	
	
	private boolean isTerminateMessage(Message msg) {
		return msg.getBody().equals("terminate");
	}
	
	 private void terminateManager() {
		 System.out.println("::MANAGER:: termination of manager");
	        requestExequtor.shutdown();  //wait for all job to finish
	        try {
	            requestExequtor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	        } catch (Exception e) {
	            System.out.println("*****MANAGER*****  Waiting threads finish FAILED error: " + e.getMessage());
	        }
	     // TODO Auto-generated method stub
	}
	 
	public static void main(String[] args) {		
		Manager manager = new Manager();		

		while (!manager.isTerminate) {
            manager.pullMessageAndDeliverTaskNewThread();
			manager.checkIfTaskFinished();
        }
        manager.terminateManager();		
	}

}
