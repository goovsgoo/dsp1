package dsp1_v1;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import dsp1_v1.AWSHandler.QueueType;

public class Manager {

	private AWSHandler 	aws;
	private  boolean isTerminate = false; 
	private int tweetsPerWorker;
	private Map<String, Integer> expectedResultsNum;
	
	public Manager() {
		aws = new AWSHandler();
	}
	
	private void pullMessageAndDoTask() {
		 System.out.println("::MANAGER:: Get file from local");

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
	        	String fileNameInS3 = message.getBody();
	        	int numTweets = distributeWork(aws.downloadFileFromS3(fileNameInS3));
	        	aws.deleteMessageFromSQS(message, QueueType.LocalToManager);
	        	tweetsPerWorker = Integer.parseInt(message.getMessageAttributes().get("tweetsPerWorker").getStringValue());
	        	int numWorkers = (int)Math.ceil(numTweets/(double)tweetsPerWorker);
	        	aws.startWorkers(numWorkers);
	        }
	}

	private void reduce() {
		Message message = null;		
		while ((message = aws.pullMessageFromSQS(QueueType.WorkerToManager)) != null) {
			
		}
	}
	/**
	 * push every tweet in the file to SQS, and returns the number of workers necessary for that job
	 * @param tweetsFile file input streem of tweets from S3
	 * @return number of tweets
	 */
	private int distributeWork(InputStream tweetsFile) {	
		try {				
	        BufferedReader reader = new BufferedReader(new InputStreamReader(tweetsFile));
	        String taskID = "task_" + UUID.randomUUID().toString();
	        String line;
	        List<Message> messages = new ArrayList<Message>();
	        int countTweets = 0;
	        while ((line = reader.readLine()) != null) {
	        	++countTweets;
	        	Message msg = new Message();
	        	msg.setBody(line);	        	
	        	MessageAttributeValue attr = new MessageAttributeValue()
	        		.withDataType("String")
	        		.withStringValue(taskID);
	        	msg.addMessageAttributesEntry("taskID", attr);
	        	messages.add(msg);	        	
	        }		        
	        aws.pushMessagesToSQS(messages, QueueType.ManagerToWorker);	        
	        return countTweets;
		}
		catch (Exception ex) {
			System.out.println(ex.getMessage());
			return 0;
		}
	}
	
	private boolean isTerminateMessage(Message msg) {
		return msg.getBody().equals("terminate");
	}
	
	 private void terminateManager() {
		// TODO Auto-generated method stub
		
	}
	 
	public static void main(String[] args) {		
		Manager manager = new Manager();
		while (!manager.isTerminate) {
            manager.pullMessageAndDoTask();
        }
        manager.terminateManager();		
	} 
}
