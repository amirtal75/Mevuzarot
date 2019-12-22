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

    static String summeryFilesIndicatorQueueUrl;
    static String QueueUrlLocalApps;
    ArrayList<String> inputFiles;
    static AtomicBoolean restartManager = new AtomicBoolean(false); // is needed?

    public LocalApp(String inputFiles) {
        ArrayList<String> files = new ArrayList<>();
        files.add(inputFiles);
        this.inputFiles = files;
    }

    public LocalApp(ArrayList<String> inputFiles) {
        this.inputFiles = inputFiles;
    }

    public void run() {
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
            //Gson gson = new Gson();
           // Instance ec2Instance = null;
            System.out.println("get result: "+ec2.getInstances("manager")) ;
            if (ec2.getInstances("manager").isEmpty()) {
                try {
                    System.out.println("Creating manager from local app");
                    QueueUrlLocalApps = queue.createQueue();
                    summeryFilesIndicatorQueueUrl = queue.createQueue();
                    // Manager userdata
                    String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
                    String unzip = getProject + "sudo unzip -o master.zip\n";
                    String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
                    String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
                    String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
                    String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
                    String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";

                    String createManagerArgsFile = "sudo touch src/main/java/managerArgs.txt\n";
                    String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
                    String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueueUrl + " >> src/main/java/managerArgs.txt\n";

                    String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + filedata + createAndRunProject;
                    System.out.println("In LocalAPP: " + Thread.currentThread());
                    System.out.println("Local Queue: " + QueueUrlLocalApps + ", Summary Queue: " + summeryFilesIndicatorQueueUrl);

                    // First created instance = manager
                    Instance instance = ec2.createInstance(1, 1, userdata).get(0);
                    ec2.attachTags(instance, "manager");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }


            else System.out.println("manager not created");
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
                        //System.out.println(obj.getTitle() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() + "\n");
                        String towrite = obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating()
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
                    System.out.println("inside loop");
            /*if (ec2.getInstances("manager").get(0).getState().getName().equals("terminated")){
                System.out.println("Manager is dead !!!!!!!!!!!!!");
                createManager(s3, ec2, QueueUrlLocalApps, summeryFilesIndicatorQueue, ec2Instance);
             }*/
                    System.out.println("hi im here");
                    String currMessageName;
                    System.out.println("trying to receive mesagee from: " + summeryFilesIndicatorQueueUrl);
                    List<Message> messages = queue.recieveMessage(summeryFilesIndicatorQueueUrl);
                    System.out.println("the message is : " + messages.isEmpty());
                    System.out.println("after receving message " + messages.size());
                    for (Message msg : messages) {
                        currMessageName = msg.getBody().split("@")[0]; // the input file name
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
                                createHTML(path,resultsToHTML);
                                System.out.println("stopping localapp");
                                summeryFileIsReady = true;
                                queue.deleteMessage(summeryFilesIndicatorQueueUrl, msg);
                            }
                        }

                    }
                    System.out.println("sleep local");
                   Thread.currentThread().sleep(3000);
//            Thread.sleep(60);
                }
            System.out.println("ending the run");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void createHTML(String path, String[] inputRepresentation) throws IOException {
        //String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment + "@" + reviewLink;
        System.out.println("the size of the input representation is " + inputRepresentation.length);
        String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
        StringBuilder html = new StringBuilder("<html>\n" + "<body>");
        for (String str : inputRepresentation) {
            String[] currReviewAttributes = str.split("@");
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
                new FileOutputStream("html_output.html"), "utf-8"))) {
            writer.write(html.toString());
        }
    }



//    public static void main(String[] args) throws IOException {
//        //String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;
//        String[] inputRepresentation = {"1234","1111","false","the book is amazing , it was a pleasure to read it",
//                "bbbbbbb","3" };
//
//        String inputFileId;
//        String reviewId;
//        String isSarcastic;
//        String reviewText;
//        String reviewEntities;
//        int reviewSentiment;
//
//        String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
//        StringBuilder html = new StringBuilder("<html>\n" + "<body>");
//        //for (String str : inputRepresentation) {
//        //String[] currReviewAttributes = str.split("@");
//        inputFileId = inputRepresentation[0];
//        reviewId = inputRepresentation[1]; // do we need to write it on the html file?
//        isSarcastic = inputRepresentation[2];
//        reviewText = inputRepresentation[3];
//        reviewEntities = inputRepresentation[4];
//        reviewSentiment = Integer.parseInt(inputRepresentation[5]);
//        html.append("<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + reviewText + "</h1>" +
//                "<h1>" + reviewEntities + " " + isSarcastic + "</h1>");
//        html.append("</body>\n" + "</html>");
//
//        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
//                new FileOutputStream("html_output.html"), "utf-8"))) {
//            writer.write(html.toString());
//        }
//    }

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