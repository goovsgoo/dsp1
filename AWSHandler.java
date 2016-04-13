package dsp1_v1;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.sqs.AmazonSQS;

public class AWSHandler {

	private AmazonEC2 ec2;
	private AmazonSQS SQS_WtoM; // SQS Worker to Manager
	private AmazonSQS SQS_MtoW; // SQS Manager to Worker
	private AmazonSQS SQS_LtoM; // SQS Local to Manager
	private AmazonSQS SQS_MtoL; // SQS Manager to Local
	
	public enum QueueType {
		WorkerToManager,
		ManagerToWorker,
		LocalToManager,
		ManagerToLocal
	}
	
	public AWSHandler() throws Exception {
		init();				
	}

	public boolean isManagerNodeActive() {
		return false;
	}
	
	public void startManagerNode() {
		
	}
	
	public void uploadFileToS3(File file) {
		 
	}
	
	public File downloadFileFromS3() {
		return null;
	}
	
	public void pushMessageToSQS(String Msg, QueueType type) {
		
	}
	
	public String pullMessageFromSQS(QueueType type) {
		return "";
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
	}
}