import com.amazonaws.services.ec2.model.Instance;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main1 {

    public static void main(String[] args) throws IOException {
        String path = "/home/amirtal/IdeaProjects/Localapp/src/main/java/";
        BufferedReader reader = new BufferedReader(new FileReader(path + "inputFile1.txt"));
        InputFileObject inputFileObject = new InputFileObject("inputFile1.txt", 3 , reader,"test", "");
        StringBuilder builder = new StringBuilder();

        while(!inputFileObject.getRedAllLines()){
            builder.append(inputFileObject.readLine() + "\n");
        }

        System.out.println(builder.toString());


        //System.out.println(getEntities("Excellent delivery book"));
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