import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.lang.StringBuilder;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.model.Message;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker {

    Queue queue;
    String myQueueUrl1; //queue for inputJobs;
    String myQueueUrl2;//queue for outputTask from workers
    List<Message> currJobQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
    ConcurrentHashMap<Integer,InputFileObject> InputFileObjectById; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    boolean isSarcastic;

    public Worker(ConcurrentHashMap <Integer,InputFileObject> InputFileObjectById, String myQueueUrl1,String myQueueUrl2) {
        this.queue = new Queue();
        this.InputFileObjectById = InputFileObjectById;
        this.myQueueUrl1 = myQueueUrl1;
        this.myQueueUrl2 = myQueueUrl2;
    }

    public void work() {
        while (true) {
            try {
                currJobQueue = queue.recieveMessage(myQueueUrl1, 1, 10); // check about visibility
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("In Worker: ");
            Message currJob = currJobQueue.get(0);
            System.out.println("Message Received: " + currJob.getBody());
            //inputFIleID + "@" + obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n");
            String[] reviewAttributes = currJob.getBody().split("@");
            String inputFileId = reviewAttributes[0];
            String reviewId = reviewAttributes[1];
            String reviewText = reviewAttributes[2];
            String reviewRating = reviewAttributes[3];
            int sentiment = findSentiment(reviewText);
            String reviewEntities = returnEntities(reviewText);
            System.out.println("Sentiment found is: " + sentiment);
            System.out.println("Entities Discovered: " + reviewEntities);
            isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
            System.out.println("Review is sarcastic: " + isSarcastic);
            String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;

            try {
                queue.sendMessage(myQueueUrl2, result);
                System.out.println("message was sent, deleting the task");
                queue.deleteMessage(myQueueUrl1,currJob); // we need to check befor deleting if we succeed to send the message
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                 // change the bool allWorkersDone if need

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


    public  String returnEntities(String review){

        StringBuilder result = new StringBuilder();
        Properties props = new Properties();
        props.put("annotators", "tokenize , split, pos, lemma, ner");
        StanfordCoreNLP NERPipeline =  new StanfordCoreNLP(props);
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                result.append("\t-" + word + ":" + ne + "$$"); // check if works

            }
        }
        return result.toString();
    }
}
