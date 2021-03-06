package dsp1_v1;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import dsp1_v1.AWSHandler.QueueType;

public class ExecuteFindAndReduce implements Runnable {

	private AWSHandler aws;
	private Map<String, Integer> expectedResultsNum;
	private Manager manager;
	
	public ExecuteFindAndReduce(AWSHandler aws, Manager manager) {
		this.aws = aws;
		this.expectedResultsNum = manager.getExpectedResultsNum();
		this.manager = manager;
	}	
	
	/**
	 * Loop and check for new messages from the Worker.
	 * When a message received - put it in an html file
	 * When all messages of a task were received - put the html file in SQS ManagerToLocal
	 */
	@Override
	public void run() {		
		// TODO: get expectedResultsNum from S3 (in case Manager was down and re-established)
		while (!manager.getIsTerminated()) {
			List<Message> messages = aws.pullMessagesFromSQS(QueueType.WorkerToManager);
			if (messages != null) {
				for (Message message : messages) {
					String messageBody = message.getBody();
					System.out.println("::Manager:: message: " + messageBody);
					String taskID = message.getMessageAttributes().get("taskID").getStringValue();
					File htmlFile = openOrCreateHtmlFile(taskID);
					if (!messageBody.equals("Failed")) {
						addHtmlLineToFile(htmlFile, messageBody);
					}
					int expectedNum = getFromMap(taskID);
					aws.deleteMessageFromSQS(message, QueueType.WorkerToManager);
					if (expectedNum != -1) { // case Manager was down and re-established again
						putInMap(taskID, expectedNum-1);										
						if (getFromMap(taskID) == 0) {
							System.out.println("::Manager:: upload reduce file To S3 taskID: " + taskID);
							addHtmlLineToFile(htmlFile, "</body>");
							addHtmlLineToFile(htmlFile, "</html>");
							String resultsPathInS3 = "dir/" + taskID + "_result.html";
							aws.uploadFileToS3(htmlFile, resultsPathInS3);								
							Message results = new Message()
								.withBody(resultsPathInS3)
								.addMessageAttributesEntry("taskID", new MessageAttributeValue()
									.withStringValue(taskID)
									.withDataType("String"));
							aws.pushMessageToSQS(results, QueueType.ManagerToLocal);							
						}
					}
				}
			}			
		}
	}

	/**
	 * A synchronized function to put (key, value) to expectedResultsNum map
	 */
	private synchronized void putInMap(String key, int value) {
		expectedResultsNum.put(key, value);
	}
	
	/**
	 * A synchronized function to get value from expectedResultsNum map
	 */
	private synchronized int getFromMap(String key) {
		Integer num = expectedResultsNum.get(key); 
		return num == null ? -1 : num.intValue();
	}
	
	/**
	 * Creates or opens a partial html file, with name: 'reduce_(taskID).html'
	 * In case of create - add html, body tags	 	
	 * @param taskID The taskID
	 * @return html (partial) file
	 */
	private File openOrCreateHtmlFile(String taskID) {
		File file = new File ("reduce_" + taskID + ".html");
		if (!file.exists()) {
			addHtmlLineToFile(file, "<html>");
			addHtmlLineToFile(file, "<body>");
		}
		return file;
	}
	
	/**
	 * create or open file that contain all the HTML Tweet from the same local
	 * @param String taskID , message with HTML tag
	 * @return file with the massage inside
	 */
	private void addHtmlLineToFile(File file , String line)
    {	
		//File file = new File ("reduce_" + taskID + ".txt");
		FileWriter fileWriter;
		PrintWriter writer;
        try
        {
        	fileWriter = new FileWriter(file, true);	//append write mode
        	writer = new PrintWriter(fileWriter);             
            writer.println(line);            
            writer.close();
        }
        catch (Exception e)
        {
            System.out.println("::Manager:: HTML Tweet file FAILED : "+e.getMessage());
        }
    }
}
