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

public class Main1 {

    public static void main(String[] args) throws IOException {
        String test = "2 -@@@@@@@- RG35LRUWRI7FE -@@@@@@@- Enjoying my purchase -@@@@@@@- 5 -@@@@@@@- https://www.amazon.com/gp/customer-reviews/RG35LRUWRI7FE/ref=cm_cr_arp_d_rvw_ttl?ie=UTF8&ASIN=B000EVOSE4";
        String[] array = test.split(" -@@@@@@@- ");
        System.out.println(array.length);
    }

}
