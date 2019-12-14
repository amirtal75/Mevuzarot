import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalApp {

    static String summeryFilesIndicatorQueueUrl;
    ArrayList<String> inputFiles;
    BookList inputRepresentation;
    AWSCredentialsProvider credentialsProvider;
    boolean summeryFileIsReady;
    static AtomicBoolean restartManager = new AtomicBoolean(false);

    public LocalApp(ArrayList<String> inputFiles) {
        this.inputFiles = inputFiles;
        this.inputRepresentation = new BookList();
        summeryFileIsReady = false;
    }

    public void run() throws Exception {

        // Create objects and bucket
        EC2Object ec2 = new EC2Object();
        S3Bucket s3 = new S3Bucket();
        s3.createBucket();
        Queue queue = new Queue();
        String QueueUrlLocalApps = queue.createQueue(); // queue for all local apps to send messages to manager
        String summeryFilesIndicatorQueue = queue.createQueue(); // queue for all local apps to get messages from worker
        String path = "/home/ubuntu/IdeaProjects/Project1/src/main/java/";
        Gson gson = new Gson();
        Instance ec2Instance = null;


        // First created instance = manager
        ec2Instance = createManager(ec2, QueueUrlLocalApps, summeryFilesIndicatorQueue, ec2Instance);

        // Go over the list of input files
        ArrayList<parsedInputObject> inputList = new ArrayList<>();
        for (String inputFile:
                inputFiles) {
            // Create a parsed object from the input list
            inputList = parse(path + inputFile);
            String outputFilename = inputFile + UUID.randomUUID() + ".txt";
            // Write the parsed object to a file
            BufferedWriter writer = new BufferedWriter(new FileWriter(path + outputFilename));
            for(parsedInputObject obj : inputList) {
                writer.write(obj.getReview().getId() + "@" + obj.getReview().getText() + "@" + obj.getReview().getRating() +"\n"); // added rating******
            }
            //  Upload it to s3 bucket and send the filename to the manager
            s3.upload(path, outputFilename);
            queue.sendMessage(QueueUrlLocalApps, outputFilename);
        }

         // outputFilename = key ??????
        Boolean managerAnswerNotReceived = true;
        Message answer = null;
        boolean foundSumaryFile = false;
        
        while (!summeryFileIsReady){
            if (ec2.getInstances("manager").get(0).getState().getName().equals("terminated")){
                System.out.println("Manager is dead !!!!!!!!!!!!!");
                createManager(ec2, QueueUrlLocalApps, summeryFilesIndicatorQueue, ec2Instance);
            }
            String currMessageName;
            List<Message> messages = queue.recieveMessage(summeryFilesIndicatorQueueUrl);

            for (Message msg : messages) {
                currMessageName = msg.getBody().split("@")[0]; // the input file name
                for (String inputFile:
                        inputFiles) {
                    if (currMessageName.split(inputFile).length > 0) {
                        S3Object outputObject = s3.downloadObject(currMessageName + "$");
                        BufferedReader reader = new BufferedReader(new InputStreamReader(outputObject.getObjectContent()));
                        String[] resultsToHTML = reader.readLine().split("\n");

                        createHTML(resultsToHTML);
                        summeryFileIsReady = true;

                        queue.deleteMessage(summeryFilesIndicatorQueueUrl, msg);
                    }
                }

            }

            Thread.sleep(60);
        }

        if (answer == null){
            throw new Exception("Answer from Manager had an error in the file");
        }
    }

    private void createHTML(String[] inputRepresentation) throws IOException {
//String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;
        String toAdd;
        String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
        File HTMLfile = new File("Desktop");
        HTMLfile.mkdir();
        BufferedWriter bw = new BufferedWriter(new FileWriter(HTMLfile));
        StringBuilder html = new StringBuilder("<html>\n" + "<body>");
        for (String str : inputRepresentation) {
            String[] currReviewAttributes = str.split("@");
            int reviewSentiment = Integer.parseInt(currReviewAttributes[5]);
            toAdd = "<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + currReviewAttributes[3] + "</h1>" +
                    "<h1>" + currReviewAttributes[4] + " " + reviewSentiment + "</h1>";
//            html.append("<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + reviewText + "</h1>" +
//                    "<h1>" + reviewEntities + " " + currReviewAttributes[2] + "</h1>");
            html.append(toAdd);
        }

        html.append("</body>\n" + "</html>");
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("html_output.html"), "utf-8"));
                writer.write(html.toString());
            }
        catch (IOException e) {
            e.printStackTrace();}

    }

    private ArrayList<parsedInputObject> parse(String filename) {

        ArrayList<parsedInputObject> inputArray = new ArrayList<parsedInputObject>();
        Gson gson = new Gson();
        BufferedReader reader;
        ArrayList<Review> reviews;
        try {
            reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                Book dataholder = gson.fromJson(line, Book.class);
                this.inputRepresentation.add(dataholder);

                if (dataholder != null) {
                    reviews = dataholder.getReviews();
                    for (int i = 0; i < reviews.size(); i++) {
                        inputArray.add(new parsedInputObject(dataholder.getTitle(), reviews.get(i)));
                    }
                }
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return inputArray;
    }

    private Instance createManager(EC2Object ec2, String QueueUrlLocalApps, String summeryFilesIndicatorQueue, Instance manager){
        boolean hasManger = ec2.getInstances("manager").size() == 0;
        if (hasManger){
            String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip";
            String unzip = getProject + "unzip master.zip\n";
            String goToProjectDirectory = unzip + "cd Mevuzarot/Project1/\n";
            String removeSuperPom = goToProjectDirectory + "rm pom.xml\n";
            String setWorkerPom = removeSuperPom + "cp managerpom pom.xml\n";
            String buildProject = setWorkerPom + "mvn compile\n mvn package\n mvn install\n";
            String createAndRunProject = buildProject + "java -jar  target/maven-1.0-SNAPSHOT.jar\n";

            String createManagerArgsFile = "touch src/main/java/managerArgs.txt\n";
            String pushFirstArg =  createManagerArgsFile + "echo " + QueueUrlLocalApps + " >> src/main/java/managerArgs.txt\n";
            String filedata = pushFirstArg + "echo " + summeryFilesIndicatorQueue + " >> src/main/java/managerArgs.txt\n";

            String userdata = "#!/bin/bash\n" + createAndRunProject + filedata;
            manager = ec2.createInstance(1,1, userdata).get(0);
            ec2.attachTags(manager, "manager");
            return  manager;
        }
        return  null;
    } 

}