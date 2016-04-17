package dsp1_v1;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.jsoup.helper.DescendableLinkedList;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.util.Base64;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AWSHandler {

	private AmazonEC2 ec2;
	private AmazonSQS sqs;
	private AmazonS3 s3;
	private String credentialsID;
	private Map<QueueType, String> sqsURLs;
	private final int MAX_RUNNING_WORKERS = 19; // AWS allows 20 instances total, and 1 of them is the Manager.
	
	public enum QueueType {
		WorkerToManager,
		ManagerToWorker,
		LocalToManager,
		ManagerToLocal
	}
	
	public AWSHandler() {
		sqsURLs = new HashMap<QueueType, String>();
		try {
			init();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isManagerNodeActive() {
		DescribeInstanceStatusResult r = ec2.describeInstanceStatus();		
		return r.getInstanceStatuses().size() > 0;
	}
	
	/**
	 * starts worker instances in EC2. Number of workers to be run is calculated as such:
	 * if the number of active workers >= numWorkers -> don't run workers
	 * if the number of active workers < numWorkers -> run (numWorkers - number of active workers) workers
	 * @param numWorkers number of necessary workers for a job
	 * @return list of instance Ids
	 */
	public List<String> startWorkers(int numWorkers) {		
		DescribeInstanceStatusResult r = ec2.describeInstanceStatus();
		int activeWorkers = Math.max(r.getInstanceStatuses().size()-1, 0); // the Manager is also an instance that should be ignored
		int numWorkersToRun = Math.max(Math.min(numWorkers, MAX_RUNNING_WORKERS) - activeWorkers, 0);
		List<Instance> workers = runInstances(numWorkersToRun, numWorkersToRun,"Worker.jar");
		List<String> ids = new ArrayList<String>();
		for (Instance i : workers) {
			ids.add(i.getInstanceId());
		}
		return ids;
	}
	
	public String startManagerNode() {
		List<Instance> manager = runInstances(1, 1,"Manager.jar");
		return manager.get(0).getInstanceId();
	}

	public void uploadFileToS3(File file, String fileName) {		
		String bucketName = credentialsID.toLowerCase() + "mybucket";
		s3.createBucket(bucketName);		
		s3.putObject(new PutObjectRequest(bucketName, fileName, file));
	}
	
	public InputStream downloadFileFromS3(String key) {
		String bucketName = credentialsID.toLowerCase() + "mybucket";
		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
		return object.getObjectContent();
	}
	
	public void pushMessageToSQS(Message msg, QueueType type) {
		String queueUrl = sqsURLs.get(type);
		SendMessageRequest r = new SendMessageRequest();
		r.setQueueUrl(queueUrl);
		r.setMessageBody(msg.getBody());
		r.setMessageAttributes(msg.getMessageAttributes());
		sqs.sendMessage(r);		
	}
	
	public void pushMessagesToSQS(List<Message> messages, QueueType type) {
		String queueUrl = sqsURLs.get(type);
		
		// There is a limitation of 10 messages in a single request
		List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
		for (int i = 0; i < messages.size(); i++) {
			Message msg = messages.get(i);		
			SendMessageBatchRequestEntry e = new SendMessageBatchRequestEntry();
			e.setMessageAttributes(msg.getMessageAttributes());
			e.setMessageBody(msg.getBody());
			e.setId(UUID.randomUUID().toString());
			entries.add(e);
			if (i % 10 == 9) {
				sqs.sendMessageBatch(new SendMessageBatchRequest(queueUrl, entries));
				entries = new ArrayList<SendMessageBatchRequestEntry>(); 
			}
		}				
	}
	
	public Message pullMessageFromSQS(QueueType type) {
		List<Message> messages = sqs.receiveMessage(sqsURLs.get(type)).getMessages();		
		return messages.size() > 0 ? messages.get(0) : null;
	}
	
	public void deleteMessageFromSQS(Message message, QueueType type) {
		sqs.deleteMessage(sqsURLs.get(type), message.getReceiptHandle());
	}
	
	public void terminateInstances(List<String> instanceIDs){
		ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));
	}
	
	private List<Instance> runInstances(int min, int max, String jarName) {
		RunInstancesRequest request = new RunInstancesRequest("ami-08111162", min, max);
        request.setInstanceType(InstanceType.T2Micro.toString());   
        try {
			request.withSecurityGroups("default").withUserData(getUserData(jarName));
		} catch (IOException e) {
			System.out.println("::AWS:: got exception - getUserData " + e.getMessage());
		}
        List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();        
        System.out.println("Launch instances: " + instances);
        return instances;
	}
	
	private String getUserData(String jarName) throws IOException {

        List<String> lines = Files.readAllLines(Paths.get("/tmp/bin/jar/properties.csv"), Charset.forName("UTF-8"));
        String accessKey = lines.get(0).substring(12);
        String secretAccessKey = lines.get(1).substring(12);

        String script = "#!/bin/sh\n"
                + "BIN_DIR=/tmp\n"
                + "AWS_ACCESS_KEY_ID=" + accessKey
                + "\n"
                + "AWS_SECRET_ACCESS_KEY=" + secretAccessKey
                + "\n"
                + "AWS_DEFAULT_REGION = us-east-1\n"
                + "wget http://www.us.apache.org/dist//commons/io/binaries/commons-io-2.4-bin.zip\n"//check if needed
                + "unzip commons-io-2.4-bin.zip\n"//check if needed
                + "cd $BIN_DIR\n"
                + "mkdir -p $BIN_DIR/bin/jar\n"
                + "export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION\n"
                + "aws s3 cp s3://dspsass1/" + jarName + ".jar $BIN_DIR/bin/jar\n"
                + "echo accessKey = $AWS_ACCESS_KEY_ID > $BIN_DIR/bin/jar/properties.csv\n"
                + "echo secretKey = $AWS_SECRET_ACCESS_KEY >> $BIN_DIR//bin/jar/properties.csv\n"
                + "ls $BIN_DIR/bin/jar/\n"
                + "cat $BIN_DIR/bin/jar/properties.csv\n"
                + "java -jar $BIN_DIR/bin/jar/" + jarName + ".jar";
        String str = new String(Base64.encode(script.getBytes()));
        return str;

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
	    
	    s3 = new AmazonS3Client(credentials);
	    s3.setRegion(usEast1);
	    
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
	    
	    pullMessageFromSQS(QueueType.ManagerToLocal);
	}
}