import com.amazonaws.services.sqs.model.Message;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main1 extends ManagerSuperClass {

    public static void main(String[] args) throws Exception {
        String test = "1 -@@@@@@@- R1TK0PGD21ZO5V -@@@@@@@- false -@@@@@@@- The best of the Gummi Bears!  Be aware - this is a LOT of gummi bears!!! -@@@@@@@- [-The:O,-best:O,-of:O,-the:O,-Gummi:O,-Bears:O,-!:O,-Be:O,-aware:O,--:O,-this:O,-is:O,-a:O,-LOT:O,-of:O,-gummi:O,-bears:O,-!!!:O,] -@@@@@@@- 1 -@@@@@@@- https://www.amazon.com/gp/customer-reviews/R1TK0PGD21ZO5V/ref=cm_cr_arp_d_rvw_ttl?ie=UTF8&ASIN=B000EVOSE4";
        String delimiter = " -@@@@@@@- ";
        Queue queue=new Queue();
        String[] resultContent = test.split(delimiter);
        /*System.out.println(resultContent.length);
        for (String str: resultContent){
            System.out.println(str);
        }*/




        String QueueUrlLocalApps = "https://sqs.us-west-2.amazonaws.com/002041186709/QueueUrlLocalApps";
        String tes = "completedTasksQueue";
        queue.recieveMessage("completedTasksQueue", 1, 60);
        Message message = queue.recieveMessage("completedTasksQueue", 1, 60).get(0);
        System.out.println(message.getBody());
        System.out.flush();

        //run.set(false); ;



    }

}
