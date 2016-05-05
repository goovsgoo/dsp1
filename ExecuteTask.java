package dsp1_v1;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dsp1_v1.AWSHandler.QueueType;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

public class ExecuteTask implements Runnable {
	
	private Message message ;
	private AWSHandler aws;
	private Map<String, Integer> expectedResultsNum;
	
	public ExecuteTask(Message message , AWSHandler aws, Map<String, Integer> expectedResultsNum){
		this.message = message;
		this.aws = aws;
		this.expectedResultsNum = expectedResultsNum;
	}
	
	public void run() {

    	// Distribute the task to tweets and put them in SQS
    	String fileNameInS3 = message.getBody();
    	String taskID = message.getMessageAttributes().get("taskID").getStringValue();
    	List<Message> messagesToSend = createMessagesToSend(taskID, aws.downloadFileFromS3(fileNameInS3));
    	int numTweets = messagesToSend.size();
    	putInMap(taskID, numTweets);
    	aws.pushMessagesToSQS(messagesToSend, QueueType.ManagerToWorker);
    	
    	// Delete the task from SQS
    	aws.deleteMessageFromSQS(message, QueueType.LocalToManager);
    	
    	// Initiate workers to do the task
    	int tweetsPerWorker = Integer.parseInt(message.getMessageAttributes().get("tweetsPerWorker").getStringValue());
    	int numWorkers = (int)Math.ceil(numTweets/(double)tweetsPerWorker);    	
    	aws.startWorkers(numWorkers);
    	
    	// Delete the task from S3
    	aws.deleteFromS3(fileNameInS3);
    	return;
	} 
	
	/**
	 * A synchronized function to put (key, value) to expectedResultsNum map
	 * @param key
	 * @param value
	 */
	private synchronized void putInMap(String key, int value) {
		expectedResultsNum.put(key, value);
	}
	
	/**
	 * Creates and returns a list of messages of tweets from the input stream
	 * @param tweetsFile file input stream of tweets from S3
	 * @return List of messages
	 */
	private List<Message> createMessagesToSend(String taskID, InputStream tweetsFile) {
		try {					
	        BufferedReader reader = new BufferedReader(new InputStreamReader(tweetsFile));	        
	        String line;
	        List<Message> messagesToSend = new ArrayList<Message>();	        
	        while ((line = reader.readLine()) != null) {	        	
	        	Message msg = new Message();
	        	msg.setBody(line);	        	
	        	MessageAttributeValue attr = new MessageAttributeValue()
	        		.withDataType("String")
	        		.withStringValue(taskID);
	        	msg.addMessageAttributesEntry("taskID", attr);
	        	messagesToSend.add(msg);	        	
	        }	
	        return messagesToSend;
		}
		catch (Exception ex) {
			System.out.println(ex.getMessage());
			return null;
		}		
	}

}
