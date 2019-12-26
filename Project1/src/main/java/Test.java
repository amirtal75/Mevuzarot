import com.google.gson.Gson;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public
class Test implements Runnable{

    public
    Test(String inputLine) {
        this.inputLine = inputLine;
    }

    String inputLine = "";


    @Override
    public
    void run() {
        System.out.println(getEntities(inputLine));
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


        String entities = "[";
        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                entities += "\t-";
                entities += word;
                entities += ":";
                entities += ne;
                entities += ",";
            }
        }
        entities+="]";
        return entities.toString();
    }

}
