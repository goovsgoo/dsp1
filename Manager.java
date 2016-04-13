package dsp1_v1;

import com.amazonaws.AmazonServiceException;



public class Manager {

	private static AWSPort awsPort;
	
	private String mTLQueue;
    private String lTMQueue;
    private String mTWQueue;
    private String wTMQueue;
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public Manager(){
	}
	
	
	//get new Queues 1.ManagerLocalQueue 2.LocalManagerQueue
	private void initQueues() throws AmazonServiceException{
		
		
        System.out.println("::MANAGER:: init queues ManagerLocalQueue & LocalManagerQueue");
        
        mTLQueue = awsPort.createSQS("MTLQueue");
        lTMQueue = awsPort.createSQS("LTMQueue");
        mTWQueue = awsPort.createSQS("MTWQueue");
        wTMQueue = awsPort.createSQS("WTMQueue");
        
        
        awsPort.setQueueVisibility(fromWorkersQueue, 180);
        awsPort.setQueueVisibility(toWorkersQueue, 180);
    }
	
}
