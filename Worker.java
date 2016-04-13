package dsp1_v1;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
 
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



public class Worker {

	StanfordCoreNLP  sentimentPipeline = null;
	StanfordCoreNLP  NERPipeline = null;
	
	public Worker(){
		//*Sentiment Analysis*//
		Properties propsSentiment = new Properties();
		propsSentiment.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline =  new StanfordCoreNLP(propsSentiment);
		
		//*Named Entity Recognition*//
		Properties propsRecognition = new Properties();
		propsRecognition.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline =  new StanfordCoreNLP(propsRecognition);
		
	}
	
	public int findSentiment(String tweet) {
		 
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
	
	
	public String findEntities(String tweet){
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
	
	public String parsingTweetFromWeb(String tweetLink){
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
	
	
//	public static void main(String[] args) {
//		//String tweet = "Deep Fried Hamburger Helper Burger Recipe - HellthyJunkFood http://t.co/o2pyv9d4O2";
//		Worker worker1 = new Worker();
//	
//		String tweet = worker1.parsingTweetFromWeb("https://www.twitter.com/BarackObama/status/710517154987122689");
//		
//		int mainSenti=worker1.findSentiment(tweet);
//
//		//find Color
//		String color = "black";
//		switch (mainSenti) {
//	        case 0:  color = "dark red";
//	        		 break;
//	        case 1:  color = "red";
//	                 break;
//	        case 2:  color = "black";
//	                 break;
//	        case 3:  color = "light green";
//	                 break;
//	        case 4:  color = "dark green";
//	                 break;
//		}
//		
//		String Entities = worker1.findEntities(tweet);
//		
//		String htmlTag = "<p><b><font color= \"" + color + "\">" + tweet + "</font></b>" + Entities + "</p>";	
//		System.out.println(htmlTag);
//		
//	}

}
