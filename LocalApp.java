package dsp1_v1;

import java.io.File;
import java.util.List;

import dsp1_v1.AWSHandler.QueueType;

public class LocalApp {
	
	public LocalApp(String path) throws Exception {
		AWSHandler handler = new AWSHandler();
		File file = new File(path);
		if (file.exists()) {
			handler.uploadFileToS3(file);
			handler.pushMessageToSQS("imageLinks.txt", QueueType.LocalToManager);
			if (!handler.isManagerNodeActive()) {
				handler.startManagerNode();
			}
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = args[0];
		try { 
			LocalApp la = new LocalApp(path);
			//AWSHandler handler = new AWSHandler();
			//handler.put
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
