import com.amazonaws.services.sqs.model.Message;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Worker {

    public static void main(String[] args) throws InterruptedException {
        String delimiter = " -@@@@@@@- ";
        System.out.println("In Worker:\n");
        Queue queue = new Queue();
        List<Message> currJobQueue = new ArrayList<>(); //at each moment holds one message from the sqs
        boolean isSarcastic;
        BufferedReader reader;
        String receivedTasks = "";
        String completedTasks = "";
        String path = "/home/ubuntu//Mevuzarot-master/Project1/src/main/java/";


        // Read the Queue names from the managerArgs file
        try {
            reader = new BufferedReader(new FileReader(path + "workerArgs.txt"));
            receivedTasks = reader.readLine();
            completedTasks = reader.readLine();
            reader.close();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
        int i = 1;
        System.out.println("receivedTasks queue" + receivedTasks + "\n completedTasks queue" + completedTasks);
        while (true) {

            try {
                currJobQueue = queue.recieveMessage(receivedTasks, 1, 60); // check about visibility
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            if(!currJobQueue.isEmpty()) {
                Message currJob = currJobQueue.get(0);
                System.out.println("Message Received: " + currJob.getBody() +"\n");
                //inputFIleID + delimiter + obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() + + obj.getReview().getLink() +"\n");
                String[] reviewAttributes = currJob.getBody().split(delimiter);
                System.out.println("review attribues length: " + reviewAttributes.length);
                String inputFileId = reviewAttributes[0];
                String reviewId = reviewAttributes[1];
                String reviewText = reviewAttributes[2];
                String reviewRating = reviewAttributes[3];
                System.out.println("Finding sentiment for the message: " + currJob.getBody() + "\n");
                int sentiment = findSentiment(reviewText);
                System.out.println("Finding entities");
                String reviewEntities = getEntities(reviewText);
                System.out.println("Sentiment found is: " + sentiment);
                System.out.println("Entities Discovered: " + reviewEntities);
                isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
                String reviewLink = reviewAttributes[4];
               // System.out.println("Review is sarcastic: " + isSarcastic);
                String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + reviewEntities + delimiter + sentiment +delimiter + reviewLink;
                //String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + sentiment;
                System.out.println("number of result ; "+ i + "the result is " + result);
                i++;
                try {
                    System.out.println("sending the result of worker to the completed queue: " + completedTasks);
                    queue.sendMessage(completedTasks, result);
                    //System.out.println("message was sent, deleting the task");
                    queue.deleteMessage(receivedTasks, currJob); // we need to check befor deleting if we succeed to send the message
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            else{
                System.out.println("Queus is empty");
                Thread.sleep(1000);
                break;
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

        System.out.println("Crash test 0\n\n");
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        System.out.println("Crash test 1\n\n");

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        System.out.println("Crash test 2\n\n");

        StringBuilder entities = new StringBuilder().append("[");
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

