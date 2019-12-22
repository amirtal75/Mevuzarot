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
        String inputLines = "R14D3WP6J91DCU@Amir went on vacation to USA.@5\n" +
                "R1RUXYHCSZSHJ0@Cute lift-the-flap book. My 9 month old loves it!@5\n" +
                "R367OTNJ8I36FS@I can't say much about this except that it was a gift for a great granddaughter.  My granddaughter tells me that she reads this to her eighteen month old daughter, and they both enjoy it enough to share it over and over again.  I read it too and found it a fun story.@5\n" +
                "RJ62XWHGDIF7@As soon as I got this package, I gave it to my 18 month old son to open and he made me read it to him about a dozen times. He loves it! He's obsessed with his belly button, so this is the perfect book for him. He's also obsessed with peek-a-boo, so he loves lifting the flaps. It was a definite hit!@5\n" +
                "R1L8O7RM8DD6J3@The book is sturdy and well made. My baby likes to carry her books around and they are routinely dropped. It has held up really well. She loves the flaps and finding the hands, feet, etc. My only critique is that the body parts that are highlighted are the smallest item on the whole page. As a visual representation for teaching a child it seems weird that the focal points are not scaled to be more familiarly identifiable.@4\n" +
                "RGMH5ROASTBS8@Well-made children's book that an child will love.@5\n" +
                "RW6ORU2M3C461@I bought this book for a newborn. She loved when our parents read to her as an infant. She's now almost two and loves playing with it herself. I got it based on the positive reviews. I'm so glad I did. Will buy it again and highly recommend@5\n" +
                "R3R9QKZ61DMKGF@This gift was liked by the child and parents.@5\n" +
                "R3GCFAIUL8BI3Y@This book is adorable. My toddler loves it.@5\n" +
                "R1IKZK5S0DCKZ0@My daughter loves lifting the flaps herself! She's almost a year. Great colorful pictures. I'm never disappointed with Karen Katz!@5\n";

        String[] lines = inputLines.split("\n");
        String review = "";
        String entities = "";
        for (String line :
             lines) {
            review = line.split("@")[1];
            entities = getEntities(review);
            System.out.println(entities + "\n");
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
