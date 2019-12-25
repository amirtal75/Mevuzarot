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

    String summeryFilesIndicatorQueue;
    String QueueUrlLocalApps;
    String inputFile;
    String path;
    String terminationIndicator;


    public LocalApp(String path, String inputFile, String terminationIndicator) {
        ArrayList<String> files = new ArrayList<>();
        this.inputFile = inputFile;
        this.QueueUrlLocalApps = "QueueUrlLocalApps";
        this.summeryFilesIndicatorQueue = UUID.randomUUID().toString();
        this.path = path;
        this.terminationIndicator = terminationIndicator;
    }

    public void run() {
        String delimiter = " -@@@@@@@- ";
        try {
            System.out.println("In local App " + Thread.currentThread().getId());
            boolean summeryFileIsReady = false;
            EC2Object ec2 = new EC2Object();
            S3Bucket s3 = new S3Bucket();
            Queue queue = new Queue();
            ArrayList<parsedInputObject> inputList = new ArrayList<>();

            inputList = parse(path + inputFile);
            String outputFilename = inputFile + UUID.randomUUID() + ".txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter(path + outputFilename));

            // Write to the output file
            String towrite;
            for (parsedInputObject obj : inputList) {
                towrite = obj.getReview().getId() + delimiter + obj.getReview().getText()
                        + delimiter + obj.getReview().getRating() + delimiter
                        + obj.getReview().getLink() + "\n";
                writer.write(towrite);
            }
            writer.flush();

            // Upload the file to the bucket and send a message to the manager local app queue
            s3.upload(path,outputFilename);
            queue.sendMessage(QueueUrlLocalApps, outputFilename + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + terminationIndicator);

            // Enter loop to wait for an answer from the manager
            String currMessageName="";
            List<Message> messages = null;
            while (!summeryFileIsReady) {

                // Check if manager crashed, reopen and resend request
                Instance instance = createManager(queue, ec2);
                if (instance == null){
                    queue.sendMessage(QueueUrlLocalApps, outputFilename + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + terminationIndicator);
                }

                // try to get an answer from the manager
                messages = queue.recieveMessage(summeryFilesIndicatorQueue,1,1);
                if (messages != null && !messages.isEmpty()) {
                    currMessageName = messages.get(0).getBody().split(delimiter)[0]; // the input file name
                }

                // Check if received a legal answer
                summeryFileIsReady = (currMessageName.contains(inputFile));
                Thread.sleep(3000);
            }

            // Download the ready answer file from the manager and read the contents to a buffer
            S3Object outputObject = s3.downloadObject(currMessageName);
            BufferedReader reader = new BufferedReader(new InputStreamReader(outputObject.getObjectContent()));
            StringBuilder stringBuilder = new StringBuilder();
            String line = "";
            while ((line = reader.readLine()) != null ){
                stringBuilder.append(line+ "\n");
            }

            // create HTML answer file from the buffer containing the content of the answer fie from the manager
            String[] resultsToHTML = stringBuilder.toString().split("\n");
            createHTML(currMessageName,resultsToHTML);

            // delete resuorces
            queue.deleteMessage(summeryFilesIndicatorQueue, messages.get(0));
            queue.deleteQueue(summeryFilesIndicatorQueue);

        }
        catch (Exception e) {
            e.printStackTrace();
        }

//            Thread.sleep(60);


        //System.out.println("ending the run");
    }

    private static void createHTML(String filename, String[] inputRepresentation) throws IOException {
        String delimiter = " -@@@@@@@- ";
        //String result = inputFileId + delimiter + reviewId + delimiter + isSarcastic + delimiter + reviewText + delimiter + reviewEntities + delimiter + sentiment + delimiter + reviewLink;
        //System.out.println("the size of the input representation is " + inputRepresentation.length);
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
        //System.out.println("in parse");
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
                        //System.out.println("title: " + dataholder.getTitle() + ", review: " + reviews.get(i).toString() );
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

    private static void createWorker(EC2Object ec2){

        if (!ec2.getInstances("worker").isEmpty()){
            return;
        }

        // worker userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp workerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject  + createAndRunProject;

        // First created instance = worker
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "worker");

    }

    private static Instance createManager(Queue queue, EC2Object ec2){

        System.out.println("No Manager Active, setting up the server");
        if (!ec2.getInstances("manager").isEmpty()){
            return null;
        }

        // Manager userdata
        String getProject = "wget https://github.com/amirtal75/Mevuzarot/archive/master.zip\n";
        String unzip = getProject + "sudo unzip -o master.zip\n";
        String goToProjectDirectory = unzip + "cd Mevuzarot-master/Project1/\n";
        String removeSuperPom = goToProjectDirectory + "sudo rm pom.xml\n";
        String setWorkerPom = removeSuperPom + "sudo cp managerpom.xml pom.xml\n";
        String buildProject = setWorkerPom + "sudo mvn  -T 4 install -o\n";
        String createAndRunProject = "sudo java -jar target/Project1-1.0-SNAPSHOT.jar\n";
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject +createAndRunProject;

        // First created instance = manager
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "manager");

        queue.createQueue("QueueUrlLocalApps");
        queue.createQueue("workerJobQueue");
        createWorker(ec2);
        return  instance;
    }

}
