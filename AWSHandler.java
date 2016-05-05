package dsp1_v1;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.util.Base64;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
	private String bucketName;
	private Map<QueueType, String> sqsURLs;
	private String selfInstanceID = null;
	
	private final int MAX_RUNNING_WORKERS = 19; // AWS allows 20 instances total, and 1 of them is the Manager.
	private final String ROOT_KEY_PATH = "./rootkey.csv";
	
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

	// TODO: check with tag
	public boolean isManagerNodeActive() {
		synchronized (ec2) {
			DescribeInstanceStatusResult r = ec2.describeInstanceStatus();		
			return r.getInstanceStatuses().size() > 0;	
		}		
	}
	
	/**
	 * starts worker instances in EC2. Number of workers to be run is calculated as such:
	 * if the number of active workers >= numWorkers -> don't run workers
	 * if the number of active workers < numWorkers -> run (numWorkers - number of active workers) workers
	 * @param numWorkers number of necessary workers for a job
	 * @return list of instance Ids
	 */
	public List<String> startWorkers(int numWorkers) {		
		
		// Calculate how many workers (instances) to be started
		int activeWorkers = 0;
		int numWorkersToRun = 0;
		synchronized (ec2) {
			DescribeInstanceStatusResult r = ec2.describeInstanceStatus();
			activeWorkers = Math.max(r.getInstanceStatuses().size()-1, 0); // the Manager is also an instance that should be ignored
			numWorkersToRun = Math.max(Math.min(numWorkers, MAX_RUNNING_WORKERS) - activeWorkers, 0);	
		}		
		
		// run instances
		List<Instance> workers = runInstances(numWorkersToRun, numWorkersToRun,"Worker.jar");
		
		// return Ids
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
		synchronized(s3) {				
			s3.putObject(new PutObjectRequest(bucketName, "dir/" + fileName, file));
		}
	}
	
	public void deleteFromS3(String fileName) {
		synchronized (s3) {			
			s3.deleteObject(bucketName, "dir/" + fileName);	
		}		
	}
	
	public InputStream downloadFileFromS3(String key) {
		synchronized (s3) {			
			S3Object object = s3.getObject(new GetObjectRequest(bucketName, "dir/" + key));
			return object.getObjectContent();	
		}		
	}
	
	public void pushMessageToSQS(Message msg, QueueType type) {
		synchronized (sqs) {
			String queueUrl = sqsURLs.get(type);
			SendMessageRequest r = new SendMessageRequest();
			r.setQueueUrl(queueUrl);
			r.setMessageBody(msg.getBody());
			r.setMessageAttributes(msg.getMessageAttributes());
			sqs.sendMessage(r);	
		}			
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
				synchronized (sqs) {
					sqs.sendMessageBatch(new SendMessageBatchRequest(queueUrl, entries));	
				}				
				entries = new ArrayList<SendMessageBatchRequestEntry>(); 
			}
		}				
	}
	
	public Message pullMessageFromSQS(QueueType type) {
		List<Message> messages =  pullMessagesFromSQS(type);		
		return messages != null ? messages.get(0) : null;
	}

	public void turnMessageVisible(Message message, QueueType type) {
		synchronized (sqs) {
			sqs.changeMessageVisibility(sqsURLs.get(type), message.getReceiptHandle(), 1);
		}	    	  
	}
	
	public List<Message> pullMessagesFromSQS(QueueType type) {
		//synchronized (sqs) {
			ReceiveMessageRequest r = new ReceiveMessageRequest(sqsURLs.get(type));
			List<String> l = new ArrayList<String>();
			l.add("All");		
			r.setMessageAttributeNames(l);		
			List<Message> messages = sqs.receiveMessage(r).getMessages();		
			return messages.size() > 0 ? messages : null;	
		//}		
	}
	
	public boolean isSQSEmpty(QueueType type) {

		return false;
	}
	
	public void deleteMessageFromSQS(Message message, QueueType type) {
		synchronized (sqs) {
			sqs.deleteMessage(sqsURLs.get(type), message.getReceiptHandle());	
		}		
	}
	
	public void terminateInstances(List<String> instanceIDs){
		synchronized (ec2) {
			ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));	
		}		
	}
	
	public void terminateSelf() {		
		synchronized (ec2) {
			String selfId = getSelfInstanceID();
			if (selfId != "") {
				List<String> l = new ArrayList<String>();
				l.add(selfId);
				ec2.terminateInstances(new TerminateInstancesRequest(l));	
			}			
		}		
	}
	
	private List<Instance> runInstances(int min, int max, String jarName) {
		RunInstancesRequest request = new RunInstancesRequest("ami-08111162", min, max);       
        try {
			request.withSecurityGroups("default")
				.withUserData(getUserData(jarName))
				.withKeyName("home")
				.withInstanceType(InstanceType.T2Small.toString());
		} catch (IOException e) {
			System.out.println("::AWS:: got exception - getUserData " + e.getMessage());
		}
        System.out.println("Launching " + max + " instances...");
        synchronized (ec2) {
        	List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();			           
	        System.out.println("Launched instances: " + instances);        
	        return instances;
        }
	}
	
	private String getUserData(String jarName) throws IOException {
        String script = "#!/bin/bash\n"  
                + "BIN_DIR=/tmp\n"
                + "cd $BIN_DIR\n"
                + "wget https://s3.amazonaws.com/akiai3bmpkxyzm2gf4gamybucket/rootkey.zip\n"
        		+ "unzip -P awsswa rootkey.zip\n"                
        		+ "wget https://s3.amazonaws.com/akiai3bmpkxyzm2gf4gamybucket/dsp1_v1_lib.zip\n"
        		+ "unzip dsp1_v1_lib.zip\n"           		
        		+ "wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0-models.jar\n"
        		+ "mv stanford-corenlp-3.3.0-models.jar dsp1_v1_lib\n"
        		+ "wget https://s3.amazonaws.com/akiai3bmpkxyzm2gf4gamybucket/" + jarName + "\n" 
                + "java -jar -Xms768m -Xmx1024m $BIN_DIR/" + jarName;
        String str = new String(Base64.encode(script.getBytes()));
        return str;

    }
	
	private String getSQSName(QueueType type) {
		return type.toString() + "_" + credentialsID;		
	}
	
	public String getSelfInstanceID() {
		URL metaurl = null;
		if (selfInstanceID != null)
			return selfInstanceID;
		
		try {
			metaurl = new URL("http://169.254.169.254/latest/meta-data/instance-id");			
			BufferedReader br = new BufferedReader(new InputStreamReader(metaurl.openStream()));
			String line = null;			
			while ((line = br.readLine()) != null) {
				selfInstanceID = line;
				return line;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * deletes all SQS queues, and all S3 files which are not necessary for initialization
	 */
	public void cleanSQSandS3 () {
		
		// delete all queues
		sqs.deleteQueue(sqsURLs.get(QueueType.LocalToManager));
		sqs.deleteQueue(sqsURLs.get(QueueType.ManagerToLocal));
		sqs.deleteQueue(sqsURLs.get(QueueType.ManagerToWorker));
		sqs.deleteQueue(sqsURLs.get(QueueType.WorkerToManager));
		
		// delete folder 'dir'
	    for (S3ObjectSummary file : s3.listObjects(bucketName, "dir").getObjectSummaries()){
	        s3.deleteObject(bucketName, file.getKey());
	      }
	    
	}
	
	private void init() throws Exception {
 	    AWSCredentials credentials = null; 	   
	    try {
	    	System.out.println("Loading credentials...");
	    	credentials = new PropertiesCredentials(new File(ROOT_KEY_PATH));	    	
	        //credentials = new ProfileCredentialsProvider("default").getCredentials();
	        credentialsID = credentials.getAWSAccessKeyId();
	        System.out.println("Credentials Loaded. Key: " + credentialsID);
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
	    this.bucketName = credentialsID.toLowerCase() + "mybucket";
		s3.createBucket(bucketName);
		
	    sqs = new AmazonSQSClient(credentials);
	    sqs.setRegion(usEast1);	    
	    Map<String,String> attr = new HashMap<String,String>();
	    attr.put("ReceiveMessageWaitTimeSeconds", "20");
	    CreateQueueRequest req = new CreateQueueRequest().withAttributes(attr);
	    String mtl = sqs.createQueue(req.withQueueName(getSQSName(QueueType.ManagerToLocal))).getQueueUrl();
	    String ltm = sqs.createQueue(req.withQueueName(getSQSName(QueueType.LocalToManager))).getQueueUrl();
	    String mtw = sqs.createQueue(req.withQueueName(getSQSName(QueueType.ManagerToWorker))).getQueueUrl();
	    String wtm = sqs.createQueue(req.withQueueName(getSQSName(QueueType.WorkerToManager))).getQueueUrl();
	    sqsURLs.put(QueueType.ManagerToLocal, mtl);    
	    sqsURLs.put(QueueType.LocalToManager, ltm);
	    sqsURLs.put(QueueType.ManagerToWorker, mtw);
	    sqsURLs.put(QueueType.WorkerToManager, wtm);		    	   
	}
}