package dsp1_v1;

import java.io.File;
import java.util.List;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import dsp1_v1.AWSHandler.QueueType;

public class LocalApp {
	
	private String fileNameInS3;
	
	public LocalApp(String path, String tweetsPerWorker) {
		AWSHandler handler = new AWSHandler();
		File file = new File(path);
		if (file.exists()) {			
			fileNameInS3 =  UUID.randomUUID().toString() + "_" + file.getName().replace("\\", "_").replace("/", "_").replace(":", "_");
			handler.uploadFileToS3(file, fileNameInS3);
			Message msg = new Message();
			msg.setBody(fileNameInS3);
			msg.addMessageAttributesEntry("tweetsPerWorker", new MessageAttributeValue().withDataType("String").withStringValue(tweetsPerWorker));
			handler.pushMessageToSQS(msg, QueueType.LocalToManager);
			if (!handler.isManagerNodeActive()) {
				handler.startManagerNode();
			}
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		String tweetsPerWorker = args[2];
		LocalApp la = new LocalApp(inputFilePath, tweetsPerWorker);
	}

}
