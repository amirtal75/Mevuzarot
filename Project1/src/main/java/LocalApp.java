import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalApp implements Runnable{

    String summeryFilesIndicatorQueueUrl;
    String QueueUrlLocalApps;
    ArrayList<String> inputFiles;
    static AtomicBoolean restartManager = new AtomicBoolean(false); // is needed?

    public LocalApp(String inputFiles, String QueueUrlLocalApps, String summeryFilesIndicatorQueueUrl) {
        ArrayList<String> files = new ArrayList<>();
        files.add(inputFiles);
        this.inputFiles = files;
        this.summeryFilesIndicatorQueueUrl = summeryFilesIndicatorQueueUrl;
        this.QueueUrlLocalApps = QueueUrlLocalApps;
    }

    public LocalApp(ArrayList<String> inputFiles, String QueueUrlLocalApps, String summeryFilesIndicatorQueueUrl) {
        this.inputFiles = inputFiles;
        this.summeryFilesIndicatorQueueUrl = summeryFilesIndicatorQueueUrl;
        this.QueueUrlLocalApps = QueueUrlLocalApps;
    }

    public void run() {
        String delimiter = " -@@@@@@@- ";
        try {
            boolean summeryFileIsReady = false;
            System.out.println("In local App " + Thread.currentThread().getId());
            // Create objects and bucket
            EC2Object ec2 = new EC2Object();
            S3Bucket s3 = new S3Bucket();
            s3.createBucket();
            Queue queue = new Queue();
            String path = "/home/amirtal/IdeaProjects/Localapp/src/main/java/";
            // C:\Users\amithaim7\Documents\GitHub\Mevuzarot\Project1\src\main\java

                // Go over the list of input files
                ArrayList<parsedInputObject> inputList = new ArrayList<>();
                for (String inputFile : inputFiles) {
                    // Create a parsed object from the input list
                    System.out.println("trying to parse the file " + path + inputFile);
                    inputList = parse(path + inputFile);
                    System.out.println("\nNumber of reviews parsed: " + inputList.size());
                    String outputFilename = inputFile + UUID.randomUUID() + ".txt";
                    // Write the parsed object to a file
                    BufferedWriter writer = new BufferedWriter(new FileWriter(path + outputFilename));
                    for (parsedInputObject obj : inputList) {

                        //System.out.println(obj.getTitle() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() + "\n");
                        String towrite = obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() + delimiter
                                + obj.getReview().getLink() + "\n"; /// added obj.getReview().getLink();
                        try {
                            writer.write(towrite); // added rating******
                        } catch (Exception e){
                            e.getMessage();
                        }
                    }

                    writer.flush();

                    s3.upload(path,outputFilename);
                    queue.sendMessage(QueueUrlLocalApps, outputFilename);
                    System.out.println("done parse");
                }

                System.out.println(" entering message loop ");
                while (summeryFileIsReady == false) {
            /*if (ec2.getInstances("manager").get(0).getState().getName().equals("terminated")){
                System.out.println("Manager is dead !!!!!!!!!!!!!");
                createManager(s3, ec2, QueueUrlLocalApps, summeryFilesIndicatorQueue, ec2Instance);
             }*/
                    String currMessageName;
                    System.out.println("trying to receive mesagee from: " + summeryFilesIndicatorQueueUrl);
                    List<Message> messages = queue.recieveMessage(summeryFilesIndicatorQueueUrl,1,1);
                    System.out.println("the message is : " + messages.isEmpty());
                    System.out.println("after receving message " + messages.size());
                    for (Message msg : messages) {
                        currMessageName = msg.getBody().split(delimiter)[0]; // the input file name
                        System.out.println("the output file name is: " + currMessageName);

                        for (String inputFile : inputFiles) {
                            if (currMessageName.indexOf(inputFile) != -1) {
                                S3Object outputObject = s3.downloadObject(currMessageName);
                                BufferedReader reader = new BufferedReader(new InputStreamReader(outputObject.getObjectContent()));

                                StringBuilder stringBuilder = new StringBuilder();
                                String line = "";
                                while ((line = reader.readLine()) != null ){
                                    stringBuilder.append(line+ "\n");
                                }

                                String[] resultsToHTML = stringBuilder.toString().split("\n");
                                createHTML(currMessageName,resultsToHTML);
                                System.out.println("stopping localapp");
                                summeryFileIsReady = true;
                                queue.deleteMessage(summeryFilesIndicatorQueueUrl, msg);
                            }
                        }

                    }
                   Thread.currentThread().sleep(3000);
//            Thread.sleep(60);
                }
                queue.deleteQueue(summeryFilesIndicatorQueueUrl);
            queue.deleteQueue(QueueUrlLocalApps);
            System.out.println("ending the run");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void createHTML(String filename, String[] inputRepresentation) throws IOException {
        String delimiter = " -@@@@@@@- ";
        //String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + reviewEntities + delimiter + sentiment + delimiter + reviewLink;
        System.out.println("the size of the input representation is " + inputRepresentation.length);
        String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
        StringBuilder html = new StringBuilder("<html>\n" + "<body>");
        for (String str : inputRepresentation) {
            String[] currReviewAttributes = str.split(delimiter);
            //int reviewSentiment = Integer.parseInt(currReviewAttributes[5]);
            int reviewSentiment = Integer.parseInt(currReviewAttributes[5]);
            String isSarcestic = "";
            if(currReviewAttributes[2].equals("false")){
                isSarcestic = "not sarcastic review";
            }
            else
                isSarcestic = "sarcastic review";
            /*toAdd = "<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + currReviewAttributes[3] + "</h1>" +
                    "<h1>" + currReviewAttributes[4] + " " + reviewSentiment + "</h1>";*/
            html.append("<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + currReviewAttributes[3] + "</h1>" +
                    "<h1>" + currReviewAttributes[4] +  "</h1>" + "<h1>" + isSarcestic + "</h1>" +
                        "<h1>" + "<a href=" + currReviewAttributes[6] +  ">" + "visit" + "</a>");
        }
        html.append("</body>\n" + "</html>");

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(filename+".html"), "utf-8"))) {
            writer.write(html.toString());
        }
    }

    private ArrayList<parsedInputObject> parse(String filename) {
        System.out.println("in parse");
        ArrayList<parsedInputObject> inputArray = new ArrayList<parsedInputObject>();
        Gson gson = new Gson();
        BufferedReader reader;
        ArrayList<Review> reviews;
        try {
            reader = new BufferedReader(new FileReader(filename));
            //System.out.println("file was opened");
            String line;
            while ((line = reader.readLine()) != null) {
                //System.out.println("current line is:" + line);
                Book dataholder = gson.fromJson(line, Book.class);
                //System.out.println("gson done");



                if (dataholder != null) {
                    //System.out.println("gson not null");
                    reviews = dataholder.getReviews();
                    //System.out.println("got reviews: " + !reviews.isEmpty());
                    for (int i = 0; i < reviews.size(); i++) {
                        System.out.println("title: " + dataholder.getTitle() + ", review: " + reviews.get(i).toString() );
                        inputArray.add(new parsedInputObject(dataholder.getTitle(), reviews.get(i)));
                    }
                }
                else System.out.println("gson was null");
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return inputArray;
    }

}