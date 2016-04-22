package dsp1_v1;

import java.util.List;
import java.util.Properties;
import java.util.Date;
import java.util.ArrayList;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.PrintWriter;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import dsp1_v1.AWSHandler.QueueType;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;


public class Worker {

	private  StanfordCoreNLP  sentimentPipeline = null;
	private  StanfordCoreNLP  NERPipeline = null;
	private  AWSHandler		  aws;
    private  List<String> 	  goodLinks;
    private  List<String>	  badLinks;
	private  String 		  workerId;
	private  int 			  workerJobsDone;
	private  Date 			  workerInitTime;
	private  Date 			  workerFinishTime;
	private  long 			  workerAverageTime;
	private  long 			  workerWorkTime;
	private  boolean 		  isTerminate = false;
	
	 
	public Worker(){		
	}
	
	private void workerInit() throws FileNotFoundException, IOException{
	  //*for stat*//
			workerInitTime = new Date(System.currentTimeMillis());
			workerWorkTime = 0;
			workerId ="yoed";
			workerJobsDone = 0;
		
	  //*aws*//
			try {
				aws = new AWSHandler();
			} catch (Exception e) {
				System.out.println("*****Worker***** got exception " + e.getMessage());
			}
			goodLinks = new ArrayList<>();
			badLinks  = new ArrayList<>();
		   
	}
	
	private void analysisInit() {
 
		System.out.println("Initating Sentiment NLP...");
	      //*Sentiment Analysis*//
	      		Properties propsSentiment = new Properties();
	      		propsSentiment.put("annotators", "tokenize, ssplit, parse, sentiment");
	      		sentimentPipeline =  new StanfordCoreNLP(propsSentiment);
	      		
	      System.out.println("Initating Recognition NLP...");
	      //*Named Entity Recognition*//
	      		Properties propsRecognition = new Properties();
	      		propsRecognition.put("annotators", "tokenize , ssplit, pos, lemma, ner");
	      		NERPipeline =  new StanfordCoreNLP(propsRecognition);
		}
	
	private Message getMessageFromManager()
    {
		Message message = aws.pullMessageFromSQS(QueueType.ManagerToWorker);
        if (message != null) {
            if (message.getBody().equals("terminate")) { 
            	isTerminate = true;
            	System.out.println("*****Worker***** got Terminate Msg");
            }     
        }
        return message;						
    }
	
		
    private void analysis()
    {
        while (true)
        {
        	Message tweetLink = getMessageFromManager();
            if (tweetLink != null)
            {
                if (!isTerminate){
                    processTweetLink(tweetLink);
                } else {
                	sendAnsToManager( tweetLink, "","terminate");
                	break;
                }
            }
        }
    }
    
    private void processTweetLink(Message tweetLink)
    {
        long startTime  = 0;
        long finishTime = 0;

        //*stat*//
        startTime = System.currentTimeMillis();
        String url = tweetLink.getBody(); 
        
        try
        {         	     
        	String tweet = parsingTweetFromWeb(url);
    		int mainSenti= findSentiment(tweet);

    		//*find Color*//
    		String color = "black";
    		switch (mainSenti) {
    	        case 0:  color = "dark red";
    	        		 break;
    	        case 1:  color = "red";
    	                 break;
    	        case 2:  color = "black";
    	                 break;
    	        case 3:  color = "light green";
    	                 break;
    	        case 4:  color = "dark green";
    	                 break;
    		}

    		String Entities = findEntities(tweet);

    		String htmlTag = "<p><b><font color= \"" + color + "\">" + tweet + "</font></b>" + Entities + "</p>";	
            
    		finishTime = System.currentTimeMillis();
    		
    		workerWorkTime += finishTime - startTime;

    		goodLinks.add("job number: " + workerJobsDone + " " + url);
    		workerJobsDone++;
            sendAnsToManager(tweetLink,url, htmlTag);
        }
        catch (Exception e)
        {
            System.out.println("*****Worker***** got exception " + e.getMessage());
            badLinks.add("job number:" +workerJobsDone + ". " + url +" "+e);
            workerJobsDone++;
            finishTime = System.currentTimeMillis();
            workerWorkTime += finishTime - startTime;
            //sendAnsToManager( tweetLink, url, "Failed");
            aws.deleteMessageFromSQS(tweetLink, QueueType.ManagerToWorker);
        }
    }
	
    
    private void sendAnsToManager(Message message, String url,String htmlTag)
    {
    	Message messageToSend = new Message();
    	messageToSend.setBody(htmlTag);
    	String taskID = message.getMessageAttributes().get("taskID").getStringValue();
    	messageToSend.addMessageAttributesEntry("taskID", new MessageAttributeValue().withDataType("String").withStringValue(taskID));
        aws.pushMessageToSQS(messageToSend, QueueType.WorkerToManager);
        System.out.println("*****Worker***** send to manager: " + htmlTag);
        aws.deleteMessageFromSQS(message, QueueType.ManagerToWorker);
    }
    
	private int findSentiment(String tweet) {
		 
        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
 
            }
        }
        return mainSentiment;
	}
	
	
	private String findEntities(String tweet){
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);
 
        // run all Annotators on this text
        NERPipeline.annotate(document);
 
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
 
        String entities = "";
        
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if( !(ne.equals("O")) ) {
                	entities = entities + "," + word + ":" + ne;      
                }                
            }
        }
        //System.out.println("::Worker:: entities: " + entities);
        if( entities != null && !(entities.equals("")) ){
        	entities = "[" + entities.substring(1) + "]";
        }
        return entities;
    }
	
	private String parsingTweetFromWeb(String tweetLink){
		Document doc;
	    try {
	        // need http protocol
	        doc = Jsoup.connect(tweetLink).get();
	    } catch (IOException e) {
	    	System.out.println("::Worker:: Jsoup FAILED to get web: " + tweetLink);
	        return null;
	    }
        // get page title
        String title = doc.title();
        //remove double quote (")
        int startCite = title.indexOf('\"');
        int endCite = title.lastIndexOf('\"');	
        title=title.substring(startCite+1, endCite);
        //System.out.println(title);
        return title;
	}
	
	private void sendStat()
    {
        File file = createStatFile();
        
        if (file != null){
        	aws.uploadFileToS3(file, workerId);
        }
    }

    private File createStatFile()
    {
    	workerFinishTime = new Date(System.currentTimeMillis());

        File file = new File ("Statistics_" + workerId + ".txt");
        PrintWriter writer;
        try
        {
            writer = new PrintWriter (file);
            writer.println("Worker ID: " + workerId);
            writer.println("Start time:Finish time = " + workerInitTime + ":" + workerFinishTime);
            if (workerJobsDone != 0)
            {
                workerAverageTime = workerWorkTime / workerJobsDone;
                writer.println("Average Working Time: " + workerAverageTime +" [ms]");
            }
            else
            {
                writer.println("Average Working Time: 0" +" [ms]");
            }
            writer.println("Number of jobs handled: " + workerJobsDone);
            writer.println("Number of success URLs: " + goodLinks.size());
            writer.println("Number of failed URLs: " + badLinks.size());
            writer.println("Successful URLs:");
            for (String link  : goodLinks)
            {
                writer.println(link);
            }
            writer.println("");
            writer.println("Failed URLs:");
            for (String link  : badLinks)
            {
                writer.println(link);
            }
            writer.close();
            return file;
        }
        catch (Exception e)
        {
            System.out.println("::Worker:: statistics file FAILED : "+e.getMessage());

        }
        return null;
    }
	
	
	
	public static void main(String[] args) throws IOException {
		Worker worker = new Worker();
		
		try {
			worker.workerInit();
			worker.analysisInit();
		} catch (IOException e) {
			System.out.println("::Worker:: got exception " + e.getMessage());
		}
		
		
		System.out.println("*****Worker***** Init sec!");
		
		worker.analysis();
		
		System.out.println("*****Worker***** analysis sec!");
		
		worker.sendStat();
		System.out.println("*****Worker***** Goodbye " + worker.workerId);
	}
}
