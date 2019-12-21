import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class workerTestAAA {


    public static void main(String[] args) throws IOException {
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        String inputFilename = "inputFile1.txt";
        int idOfInputFile = 0;
        idOfInputFile++;
        InputFileObject currFileObject = new InputFileObject(idOfInputFile, inputFilename,path);
        ConcurrentHashMap<Integer,InputFileObject> InputFileObjectById = new ConcurrentHashMap<Integer,InputFileObject>();
        ArrayList<String> jobs = new ArrayList<String>();
        ArrayList<String> results = new ArrayList<String>();
        InputFileObjectById.putIfAbsent(idOfInputFile, currFileObject);
            // Create a parsed object from the input list
        System.out.println("trying to parse the file " + path + inputFilename);
            ArrayList<parsedInputObject> inputList = parse(path + inputFilename);
            String outputFilename = inputFilename + UUID.randomUUID() + ".txt";
            // Write the parsed object to a file
            BufferedWriter writer = new BufferedWriter(new FileWriter(path + outputFilename));
            for (parsedInputObject obj : inputList) {
                writer.write(obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() + "\n"); // added rating******
            }

        BufferedReader inputFileFromLocalApp = new BufferedReader(new FileReader(path+outputFilename));
        String currLine = "";
        String job = "";
        while ((currLine = inputFileFromLocalApp.readLine()) != null) {
            System.out.println(" Making a job from the current read line: " + currLine);
            // Line content: (obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n"); // added rating******
            job = idOfInputFile + "@" + currLine;
            jobs.add(job);
            }

        System.out.println("In Worker:\n");
        boolean isSarcastic;
           for(String task : jobs) {
                    System.out.println("Message Received: " + task);
                    //inputFIleID + "@" + obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n");
                    String[] reviewAttributes = task.split("@");
                    String inputFileId = reviewAttributes[0];
                    String reviewId = reviewAttributes[1];
                    String reviewText = reviewAttributes[2];
                    String reviewRating = reviewAttributes[3];
                    System.out.println("Finding sentiment");
                    int sentiment = findSentiment(reviewText);
                    System.out.println("Finding entities");
                    String reviewEntities = returnEntities(reviewText);
                    System.out.println("Sentiment found is: " + sentiment);
                    System.out.println("Entities Discovered: " + reviewEntities);
                    isSarcastic = Math.abs(sentiment - Integer.parseInt(reviewRating)) < 2;
                    System.out.println("Review is sarcastic: " + isSarcastic);
                    String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;

                   results.add(result);
                   System.out.println("message was sent, deleting the task");
                   jobs.remove(0); // we need to check befor deleting if we succeed to send the message
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

    private static ArrayList<parsedInputObject> parse(String filename) {
        System.out.println("in parse");
        ArrayList<parsedInputObject> inputArray = new ArrayList<parsedInputObject>();
        Gson gson = new Gson();
        BufferedReader reader;
        ArrayList<Review> reviews;
        try {
            reader = new BufferedReader(new FileReader(filename));
            System.out.println("file was opened");
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("current line is:" + line);
                Book dataholder = gson.fromJson(line, Book.class);
                System.out.println("gson done");


                if (dataholder != null) {
                    System.out.println("gson not null");
                    reviews = dataholder.getReviews();
                    System.out.println("got reviews");
                    for (int i = 0; i < reviews.size(); i++) {
                        inputArray.add(new parsedInputObject(dataholder.getTitle(), reviews.get(i)));
                    }
                } else System.out.println("gson was null");
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputArray;
    }

}