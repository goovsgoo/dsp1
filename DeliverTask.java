package dsp1_v1;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import dsp1_v1.AWSHandler.QueueType;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

public class DeliverTask implements Callable<Object> {
	
	private Message message ;
	private AWSHandler aws;
	private Map<String, Integer> expectedResultsNum ;
	private int tweetsPerWorker;
	
	public DeliverTask(Message messageM , AWSHandler awsM , Map<String, Integer> expectedResultsNumM , int tweetsPerWorkerM){
		message = messageM;
		aws = awsM;
		expectedResultsNum = expectedResultsNumM;
		tweetsPerWorker = tweetsPerWorkerM;
	}
	
	public Map<String, Integer> call() {

    	// Distribute the task to tweets and put them in SQS
    	String fileNameInS3 = message.getBody();
    	String taskID = "task_" + UUID.randomUUID().toString();
    	int numTweets = distributeWork(taskID, aws.downloadFileFromS3(fileNameInS3));
    	expectedResultsNum.put(taskID, numTweets);
    		        
    	// Delete the task from SQS
    	aws.deleteMessageFromSQS(message, QueueType.LocalToManager);
    	
    	// Initiate workers to do the task
    	tweetsPerWorker = Integer.parseInt(message.getMessageAttributes().get("tweetsPerWorker").getStringValue());
    	int numWorkers = (int)Math.ceil(numTweets/(double)tweetsPerWorker);
    	aws.startWorkers(numWorkers);
    	
    	return expectedResultsNum; //how to kill safety???
	} 
	
	/**
	 * push every tweet in the file to SQS, and returns the number of workers necessary for that job
	 * @param tweetsFile file input streem of tweets from S3
	 * @return number of tweets
	 */
	private int distributeWork(String taskID, InputStream tweetsFile) {	
		try {				
	        BufferedReader reader = new BufferedReader(new InputStreamReader(tweetsFile));	        
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

}
