package dsp1_v1;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import dsp1_v1.AWSHandler.QueueType;

public class LocalApp {
	
	private String fileNameInS3;
	AWSHandler handler = null;
	String outputFilePath;
	
	public LocalApp(String inputFilePath, String outputFilePath, String tweetsPerWorker, boolean shouldTerminate) {
		handler = new AWSHandler();
		this.outputFilePath = outputFilePath;		
		File file = new File(inputFilePath);
		if (file.exists()) {
			
			// Push the task to S3, SQS
			String taskID = UUID.randomUUID().toString();
			fileNameInS3 = "dir/" + taskID + "_" + file.getName().replace("\\", "_").replace("/", "_").replace(":", "_");
			handler.uploadFileToS3(file, fileNameInS3);
			Message msg = new Message();
			msg.setBody(fileNameInS3);
			msg.addMessageAttributesEntry("tweetsPerWorker", new MessageAttributeValue().withDataType("String").withStringValue(tweetsPerWorker));
			msg.addMessageAttributesEntry("taskID", new MessageAttributeValue().withDataType("String").withStringValue(taskID));
			handler.pushMessageToSQS(msg, QueueType.LocalToManager);
			
			// Start the manager if necessary
			if (!handler.isManagerNodeActive()) {
				handler.startManagerNode();
			}
			
			// Wait for results to arrive
			boolean resultsRecieved = false;
			while (!resultsRecieved) {
				Message results = handler.pullMessageFromSQS(QueueType.ManagerToLocal);
				
				// check if the results belongs to this local. 
				if (results != null && results.getMessageAttributes().get("taskID").getStringValue().equals(taskID)) {
					resultsRecieved = true;
					InputStream inputStream = handler.downloadFileFromS3(results.getBody());
					handleResults(inputStream);
					handler.deleteMessageFromSQS(results, QueueType.ManagerToLocal);
					if (shouldTerminate) {
						handler.pushMessageToSQS(new Message().withBody("terminate"), QueueType.LocalToManager);
					}
				}
				else if (results != null) {
					handler.turnMessageVisible(results, QueueType.ManagerToLocal); // makes the message visible to other locals
					try {
						Thread.sleep(5000);  // give other locals a chance to pull the message
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}		
	}

	/**
	 * saves the results from the input stream to a file	 * 
	 * @param results
	 */
	private void handleResults(InputStream in) {					
		try 
		{								
			OutputStream outputStream = new FileOutputStream(new File(outputFilePath));
			int read = 0;
			byte[] bytes = new byte[1024];
	
			while ((read = in.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
			}
			outputStream.close();
		}
		catch (IOException exception) {
			System.out.println(exception.getMessage());
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Running LocalApp args: " + args[0] + " " + args[1] + " " + args[2]);
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		String tweetsPerWorker = args[2];
		boolean shouldTerminate = args.length > 3 && args[3].equals("terminate");
		
		new LocalApp(inputFilePath, outputFilePath, tweetsPerWorker, shouldTerminate);
	}

}
