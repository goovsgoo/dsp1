package dsp1_v1;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import dsp1_v1.AWSHandler.QueueType;

public class LocalApp {
	
	private AmazonEC2 ec2;
	
	public LocalApp(String path) throws Exception {
		System.out.print("Starting...");		
		init();  
		System.out.print("Init completed");
		
		try {
			RunInstancesRequest request = new RunInstancesRequest("ami-08111162", 1, 1);
            request.setInstanceType(InstanceType.T2Micro.toString());
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
		}
		catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        } catch (Exception e) {
        	
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = args[0];
		try {
			//LocalApp la = new LocalApp(path);
			AWSHandler handler = new AWSHandler();
			handler.startManagerNode();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
