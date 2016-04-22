package dsp1_v1;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;


import dsp1_v1.AWSHandler.QueueType;

public class Manager {

	private AWSHandler 	aws;
	private  boolean isTerminate = false; 
	private Map<String, Integer> expectedResultsNum = new HashMap<String, Integer>();	
	private ExecutorService requestExecutor;
	
	public Manager() {
		aws = new AWSHandler();
		requestExecutor = Executors.newFixedThreadPool(10);
	}
	
	private Message pullMessage() {
		System.out.println("Waiting for tasks...");
    	return aws.pullMessageFromSQS(QueueType.LocalToManager);
	}
	
	private void deliverTask(Message message) {
        if(message == null){
            return;
        }
        else if (isTerminateMessage(message)) {
            System.out.println("::MANAGER:: Got [Terminate] message from local");
            isTerminate = true;
            //terminateManager();
        } 
        else {
        	System.out.println("::MANAGER:: found a task to do");	        	
        	ExecuteTask executor = new ExecuteTask(message ,aws, expectedResultsNum);
    		try {	    			
    			requestExecutor.execute(executor);
    			System.out.println("::MANAGER:: Thread DeliverTask start!");
    		} 
    		catch (Exception e) {
    			System.out.println("::MANAGER::  Thread DeliverTask FAILED error: " + e.getMessage());
    		}
        }
	}
	
	/**
	 * toggle search the queue 'WorkerToManager' to see if all messages from a task were received.
	 * In that case, envelop the responses in HTML file and sends back to the Local
	 */
	private void toggleCheckIfTaskFinished() {
		ExecuteFindAndReduce executor = new ExecuteFindAndReduce(aws, expectedResultsNum);
		requestExecutor.execute(executor);
	}
	
	private boolean isTerminateMessage(Message msg) {
		return msg.getBody().equals("terminate");
	}
	
	 private void terminateManager() {
		 System.out.println("::MANAGER:: termination of manager");
	        requestExecutor.shutdown(); //wait for all job to finish
	        try {
	            requestExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	        } catch (Exception e) {
	            System.out.println("*****MANAGER*****  Waiting threads finish FAILED error: " + e.getMessage());
	        }
	}
	 
	public static void main(String[] args) {		
		Manager manager = new Manager();				
		manager.toggleCheckIfTaskFinished();
		
		while (!manager.isTerminate) {
            Message m = manager.pullMessage();
            manager.deliverTask(m); // Deliver the task to new thread (if there is such task)			
        }
        manager.terminateManager();		
	}

}
