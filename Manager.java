package dsp1_v1;

import dsp1_v1.AWSHandler.QueueType;

public class Manager {

	private AWSHandler 	aws;
	private  boolean	isTerminate = false; 
	
	public static void main(String[] args) {
		Manager manager = new Manager();
		try{
			manager.initManager();
		} catch (Exception e) {
			System.out.println("::MANAGER:: Exception to init AWSHandler");
			 manager.terminateManager();
		}
		while (!manager.isTerminate) {
            manager.getMessageFromLocal();
        }
        manager.terminateManager();
	
		
	} 
	
	 private void getMessageFromLocal() {
		 System.out.println("::MANAGER:: Get file from local");

	        String message = aws.pullMessageFromSQS(QueueType.LocalToManager);
	        if(message == null){
	            return;
	        }
	        if (isTerminateMessage(message)) {
	            System.out.println("::MANAGER:: Got [Terminate] message from local");
	            isTerminate = true;
	            return;
	        }
	        //File file = aws.downloadFileFromS3(message);
	        ////??????????????????????????????????????????????????????????????????????????????????????????????????????????
	        /// how we do the parse?? if the Massage is only in the AWSHandler?
	        // String inputFileName = 
	       //  String localID = 
	        // int urlsPerWorker =   		
	}

	private void initManager() throws Exception  {
	        System.out.println("::MANAGER::  Start initialization of manager");
				aws = new AWSHandler();
	    }
	
}
