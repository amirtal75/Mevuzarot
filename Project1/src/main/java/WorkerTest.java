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

public class WorkerTest {
    String myQueueUrl1 = "";
    String myQueueUrl2 = "";

    public
    WorkerTest(String quque1, String queue2) {
        myQueueUrl1 = quque1;
        myQueueUrl2 = queue2;
    }

    public
    void main() throws Exception {

        Queue queue = new Queue();
        List<Message> currJobQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
        boolean isSarcastic;
        BufferedReader reader = null;


        // Read the Queue names from the managerArgs file
        try {
            reader = new BufferedReader(new FileReader("workerArgs"));


        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("receiving queue: " + myQueueUrl1);
        int job = 1;
        List<Message> currQueue = queue.recieveMessage(myQueueUrl1, 1, 1000);


        while (!currQueue.isEmpty()) { // What condition should be inside the while ?
//                try {
//                    currJobQueue = queue.recieveMessage(myQueueUrl1, 1, 10); // check about visibility
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                }
            System.out.println("In Worker: ");
            Message currJob = currQueue.get(0);

            System.out.println("Message Received: " + currJob.getBody());

            //inputFIleID + "@" + obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n");
            String[] reviewAttributes = currJob.getBody().split("@");
            System.out.println("After split: " + reviewAttributes);
            String inputFileId = reviewAttributes[0];
            String reviewId = reviewAttributes[1];
            String reviewText = reviewAttributes[2];
            String reviewRating = reviewAttributes[3];
            int sentiment = findSentiment(reviewText);
            System.out.println("the sentiment is : " + sentiment);
           // String reviewEntities = returnEntities(reviewText);
           // System.out.println("the entities are : " + reviewEntities);

            isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
            System.out.println("Review is sarcastic: " + isSarcastic);
            //String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@";//sentiment;
          //  System.out.println("job " + job + " result: " + result);
            job++;
            currQueue = queue.recieveMessage(myQueueUrl1, 1, 1000);
            try {
                //queue.sendMessage(myQueueUrl2, result);
                //System.out.println("message was sent, deleting the task");
                //queue.deleteMessage(myQueueUrl1, currJob); // we need to check befor deleting if we succeed to send the message
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static
    int findSentiment(String review) {

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


    public String getEntities(String review) {


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

        StringBuilder entities = new StringBuilder("[");
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

    public static void main(String[] args) {
        String review = " the book is wonderfull and I love it it it";
        WorkerTest worker = new WorkerTest("a","b");
        System.out.println(worker.getEntities(review));
    }
}



