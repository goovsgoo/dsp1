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
		
		try {
			workerInit();
		} catch (IOException e) {
			System.out.println("::Worker:: got exception " + e.getMessage());
		}
		analysisInit();
		
	}
	
	private void workerInit() throws FileNotFoundException, IOException{
		
	  //*for stat*//
			workerInitTime = new Date(System.currentTimeMillis());
			workerWorkTime = 0;
			//workerId = //awsHandler.getID();
			workerJobsDone = 0;
		
	  //*aws*//
			//aws = new //AWSHandler(Path, .....);constractor!	
			goodLinks = new ArrayList<>();
			badLinks  = new ArrayList<>();
		   
	}
	private void analysisInit() {
 
	      //*Sentiment Analysis*//
	      		Properties propsSentiment = new Properties();
	      		propsSentiment.put("annotators", "tokenize, ssplit, parse, sentiment");
	      		sentimentPipeline =  new StanfordCoreNLP(propsSentiment);
	      		
	      //*Named Entity Recognition*//
	      		Properties propsRecognition = new Properties();
	      		propsRecognition.put("annotators", "tokenize , ssplit, pos, lemma, ner");
	      		NERPipeline =  new StanfordCoreNLP(propsRecognition);
		}
	
	private String getTweetLinkFromManager()
    {
        String tweetLink = aws.pullMessageFromSQS(QueueType.ManagerToWorker);
        if (tweetLink != null) {
            if (isTerminateMessage(tweetLink)) {
                aws.workerTerminate();
            } 
                return tweetLink;
        }
        return null;
    }
	
    private void analysis()
    {
        while (!isTerminate)
        {
            String tweetLink = getTweetLinkFromManager();
            if (tweetLink != null)
            {
                if (isTerminateMessage(tweetLink))
                {
                	isTerminate = true;
                }
                else
                {
                    processTweetLink(tweetLink);
                }
            }
        }
    }
    
    private void processTweetLink(String tweetLink)
    {
        long startTime  = 0;
        long finishTime = 0;

        //*stat*//
        startTime = System.currentTimeMillis();
        
        try
        {  
        	//replace tweet ==>> tweetLink . whan end of work !!!!!!!!!!!!!!!!!!!!!
            String tweet = parsingTweetFromWeb("https://www.twitter.com/BarackObama/status/710517154987122689");
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
    		//System.out.println(htmlTag);
            
    		finishTime = System.currentTimeMillis();
    		workerWorkTime += finishTime - startTime;

    		goodLinks.add("job number: " + workerJobsDone + " " + tweetLink);
    		workerJobsDone++;
            sendAnsToManager(tweetLink, htmlTag);
        }
        catch (Exception e)
        {
            System.out.println("*****Worker***** got exception " + e.getMessage());
//            failedURLs.add("job number:" +workerJobsDone + ". " + tweetLink +" "+e);
//            workerJobsDone++;
//            finishTime = System.currentTimeMillis();
//            workerWorkTime += finishTime - startTime;
//            sendAnsToManager( tweetLink, "Failed");
//            aws.deleteMessageFromQueue();
        }
    }
	
    
    private void sendAnsToManager(String tweetLink,String htmlTag)
    {
        aws.pushMessageToSQS(htmlTag, QueueType.WorkerToManager);
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
        entities = "[" + entities.substring(1) + "]";
        return entities;
    }
	
	private String parsingTweetFromWeb(String tweetLink){
		Document doc;
	    try {
	
	        // need http protocol
	        doc = Jsoup.connect(tweetLink).get();
	
	        // get page title
	        String title = doc.title();
	        //remove double quote (")
	        int startCite = title.indexOf('\"');
	        int endCite = title.lastIndexOf('\"');	
	        title=title.substring(startCite+1, endCite);
	        //System.out.println(title);
	        return title;
	    
	    } catch (IOException e) {
	        e.printStackTrace();
	        return null;
	    }
	}
	
	private void sendStat()
    {
        File file = createStatFile();
        
        if (file != null){
            aws.uploadFileToS3(file);
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
		
		worker.analysis();
		worker.sendStat();
		worker.goTerminate();
	}
}
