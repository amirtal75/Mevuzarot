import com.amazonaws.services.sqs.model.Message;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Worker {
    public static void main(String[] args){
        String delimiter = " -@@@@@@@- ";
        Queue queue = new Queue();
        List<Message> currJobQueue = new ArrayList<>(); //at each moment holds one message from the sqs
        boolean isSarcastic;
        String workerJobQueue = "workerJobQueue";

        while (true) {
            try{
                if(!currJobQueue.isEmpty()) {
                    Message currJob = currJobQueue.get(0);
                    //System.out.println("Message Received: " + currJob.getBody() +"\n");
                    //inputFIleID + delimiter + obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() + + obj.getReview().getLink() +"\n");
                    String[] reviewAttributes = currJob.getBody().split(delimiter);
                    //System.out.println("review attribues length: " + reviewAttributes.length);
                    String inputFileId = reviewAttributes[0];
                    String reviewId = reviewAttributes[1];
                    String reviewText = reviewAttributes[2];
                    String reviewRating = reviewAttributes[3];
                    //System.out.println("Finding sentiment for the message: " + currJob.getBody() + "\n");
                    int sentiment = findSentiment(reviewText);
                    //System.out.println("Finding entities");
                    String reviewEntities = getEntities(reviewText);
                    //System.out.println("Sentiment found is: " + sentiment);
                    //System.out.println("Entities Discovered: " + reviewEntities);
                    isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
                    String reviewLink = reviewAttributes[4];
                    // //System.out.println("Review is sarcastic: " + isSarcastic);
                    String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + reviewEntities + delimiter + sentiment +delimiter + reviewLink;
                    //String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + sentiment;
                    //System.out.println("number of result ; "+ i + "the result is " + result);

                    System.out.println("sending the result of worker to the completed queue: " + reviewAttributes[0]);
                    queue.sendMessage(reviewAttributes[0], result);
                    ////System.out.println("message was sent, deleting the task");
                    queue.deleteMessage(workerJobQueue, currJob); // we need to check befor deleting if we succeed to send the message
                } else{
                    System.out.println("Queus is empty");
                    Thread.sleep(1000);
                }
                currJobQueue = queue.recieveMessage(workerJobQueue, 1, 60); // check about visibility
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static int findSentiment(String review) {

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP sentimentPipeline = new StanfordCoreNLP(props);
        int mainSentiment = 0;
        if (review != null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
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


    public static String getEntities(String review) {

        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);


        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);


        StringBuffer entities = new StringBuffer("[");
        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                entities.append("\t-").append(word).append(":").append(ne).append(",");
            }
        }
        entities.append("]");
        return entities.toString();
    }
}
