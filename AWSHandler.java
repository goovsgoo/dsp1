package dsp1_v1;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AWSHandler {

	private AmazonEC2 ec2;
	private AmazonSQS sqs;
	private String credentialsID;
	private Map<QueueType, String> sqsURLs;
	
	public enum QueueType {
		WorkerToManager,
		ManagerToWorker,
		LocalToManager,
		ManagerToLocal
	}
	
	public AWSHandler() throws Exception {
		sqsURLs = new HashMap<QueueType, String>();		
		init();				
	}

	public boolean isManagerNodeActive() {
		return false;
	}
	
	public List<String> startWorkers(int numWorkers) {
		List<Instance> workers = runInstances(numWorkers, numWorkers);
		List<String> ids = new ArrayList<String>();
		for (Instance i : workers) {
			ids.add(i.getInstanceId());
		}
		return ids;
	}
	
	public String startManagerNode() {
		List<Instance> manager = runInstances(1, 1);
		return manager.get(0).getInstanceId();
	}
	
	public void uploadFileToS3(File file) {
		 
	}
	
	public File downloadFileFromS3() {
		return null;
	}
	
	public void pushMessageToSQS(String msg, QueueType type) {
		String queueUrl = sqsURLs.get(type);
		sqs.sendMessage(new SendMessageRequest(queueUrl, msg));
	}
	
	public Message pullMessageFromSQS(QueueType type) {
		return sqs.receiveMessage(sqsURLs.get(type)).getMessages().get(0);
	}
	
	public void deleteMessageFromSQS(Message message, QueueType type) {
		sqs.deleteMessage(sqsURLs.get(type), message.getReceiptHandle());
	}
	
	public void terminateInstances(List<String> instanceIDs){
		ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));
	}
	
	private List<Instance> runInstances(int min, int max) {
		RunInstancesRequest request = new RunInstancesRequest("ami-08111162", min, max);
        request.setInstanceType(InstanceType.T2Micro.toString());        
        List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();        
        System.out.println("Launch instances: " + instances);
        return instances;
	}
	
	private String getSQSName(QueueType type) {
		return type.toString() + "_" + credentialsID;		
	}
	
	private void init() throws Exception {
	    /*
	     * The ProfileCredentialsProvider will return your [default]
	     * credential profile by reading from the credentials file located at
	     * (C:\\Users\\Or\\.aws\\credentials).
	     */
	    AWSCredentials credentials = null;
	    try {
	        credentials = new ProfileCredentialsProvider("default").getCredentials();
	        credentialsID = credentials.getAWSAccessKeyId();
	    } catch (Exception e) {
	        throw new AmazonClientException(
	                "Cannot load the credentials from the credential profiles file. " +
	                "Please make sure that your credentials file is at the correct " +
	                "location (C:\\Users\\Or\\.aws\\credentials), and is in valid format.",
	                e);
	    }
	
	    ec2 = new AmazonEC2Client(credentials);
	    Region usEast1 = Region.getRegion(Regions.US_EAST_1);
	    ec2.setRegion(usEast1);
	    
	    sqs = new AmazonSQSClient(credentials);
	    sqs.setRegion(usEast1);	    
	    String mtl = sqs.createQueue(new CreateQueueRequest(getSQSName(QueueType.ManagerToLocal))).getQueueUrl();
	    String ltm = sqs.createQueue(new CreateQueueRequest(getSQSName(QueueType.LocalToManager))).getQueueUrl();
	    String mtw = sqs.createQueue(new CreateQueueRequest(getSQSName(QueueType.ManagerToWorker))).getQueueUrl();
	    String wtm = sqs.createQueue(new CreateQueueRequest(getSQSName(QueueType.WorkerToManager))).getQueueUrl();
	    sqsURLs.put(QueueType.ManagerToLocal, mtl);    
	    sqsURLs.put(QueueType.LocalToManager, ltm);
	    sqsURLs.put(QueueType.ManagerToWorker, mtw);
	    sqsURLs.put(QueueType.WorkerToManager, wtm);	
	    
	    String a = pullMessageFromSQS(QueueType.ManagerToLocal);
	}
}