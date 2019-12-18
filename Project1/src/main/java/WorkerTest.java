import java.io.BufferedReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.StringBuilder;
import com.amazonaws.services.sqs.model.Message;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class WorkerTest {

    public static void main(String[] args) {
        String a = "THE BOOK WAS WONDERFULL , VERY RECOMMENDED";
        int sentiment = findSentiment(a);
            Queue queue = new Queue();
            List<Message> currJobQueue = new ArrayList<Message>(); //at each moment holds one message from the sqs
            boolean isSarcastic;
            BufferedReader reader = null;
            String myQueueUrl1 = "";
            String myQueueUrl2 = "";

            // Read the Queue names from the managerArgs file
            try {
                reader = new BufferedReader(new FileReader("workerArgs"));
                myQueueUrl1 = reader.readLine();
                myQueueUrl2 = reader.readLine();
            }
            catch (IOException e) {
                System.out.println(e.getMessage());
            }

            while (true) { // What condition should be inside the while ?
//                try {
//                    currJobQueue = queue.recieveMessage(myQueueUrl1, 1, 10); // check about visibility
//                }
//                catch (Exception e) {
//                    e.printStackTrace();
//                }
                System.out.println("In Worker: ");
 //               Message currJob = currJobQueue.get(0);
  //              System.out.println("Message Received: " + currJob.getBody());

                String testStr = "555@1234@the book was amazing i love it so much@5\n";
                System.out.println("Message Received: " + testStr);
                //inputFIleID + "@" + obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n");
                String[] reviewAttributes = testStr.split("@");
                String inputFileId = reviewAttributes[0];
                String reviewId = reviewAttributes[1];
                String reviewText = reviewAttributes[2];
                String reviewRating = reviewAttributes[3];
               // int sentiment = findSentiment(reviewText);
                System.out.println("the sentiment is : " + sentiment);
                String reviewEntities = returnEntities(reviewText);
                System.out.println("the entities are : " + reviewEntities);
               // isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
               // System.out.println("Review is sarcastic: " + isSarcastic);
              //  String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" ;//sentiment;

//                try {
//                    queue.sendMessage(myQueueUrl2, result);
//                    System.out.println("message was sent, deleting the task");
//                    queue.deleteMessage(myQueueUrl1, currJob); // we need to check befor deleting if we succeed to send the message
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
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


        public static String returnEntities(String review) {
            System.out.println("1");
            StringBuilder result = new StringBuilder();
            System.out.println("2");
            Properties props = new Properties();
            System.out.println("3");
            props.put("annotator", "tokenize , split, pos, lemma, ner");
            System.out.println("4");
            StanfordCoreNLP NERPipeline = new StanfordCoreNLP(props);
            System.out.println("5");
            // create an empty Annotation just with the given text
            Annotation document = new Annotation(review);
            System.out.println("6");

            // run all Annotators on this text
            NERPipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            for (CoreMap sentence : sentences) {
                // traversing the words in the current sentence
                // a CoreLabel is a CoreMap with additional token-specific methods
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
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



