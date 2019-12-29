import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Main {

    public static
    void main(String[] args) throws Exception {
        String QueueUrlLocalApps = "QueueUrlLocalApps";
        ArrayList<String> outputFileNameList = new ArrayList<>();
        String pathtoPtojectLocation = args[0];
        EC2Object ec2 = new EC2Object();
        Queue queue = new Queue();
        String summeryFilesIndicatorQueue = "LocalApp-"+UUID.randomUUID().toString();
        ArrayList<parsedInputObject> inputList = new ArrayList<>();
        String outputFilename = "";
        BufferedWriter writer = null;
        String delimiter = " -@@@@@@@- ";
        String towrite = "";
        S3Bucket s3 = new S3Bucket();
        String urlPrefix = "https://sqs.us-west-2.amazonaws.com/002041186709/";
        String terminatorOutputFile = "";
        // Create bucket
        new S3Bucket().createBucket();
        // Create manager and worker if not already opened
        Instance instance = createManager(queue, ec2);

        // Create the summary queue
        queue.createQueue(summeryFilesIndicatorQueue);

        int size =  args.length;
        String terminationIndicator = args[args.length-1];
        if(args[size-1].equals("terminate")){
            --size;
        }
        else terminationIndicator = "Dont Terminate";

        for (int i = 1; i < size; i++) {

            inputList = parse(args[0] + args[i]);
            System.out.println(inputList.size());
            outputFilename = args[i] + UUID.randomUUID().toString();
            outputFileNameList.add(outputFilename);

            // create Buffered Reader for the output file
            writer = new BufferedWriter(new FileWriter(args[0] + outputFilename));

            // Write to the output file
            for (parsedInputObject obj : inputList) {
                towrite = obj.getReview().getId() + delimiter + obj.getReview().getText()
                        + delimiter + obj.getReview().getRating() + delimiter
                        + obj.getReview().getLink() + "\n";
                writer.write(towrite);
            }
            writer.flush();

            // Upload the finished file to the bucket
            s3.upload(args[0],outputFilename);
            // send message to the Manager
            if (terminationIndicator.equals("terminate")){
                if (i < size-1){
                    queue.sendMessage(QueueUrlLocalApps, outputFilename + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + "Dont Terminate");
                }
                else terminatorOutputFile = outputFilename;
            }
            else queue.sendMessage(QueueUrlLocalApps, outputFilename + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + "Dont Terminate");
        }

        List<String> queues = new ArrayList<>();
        String currMessageName="";
        List<Message> messages = null;

        while (!outputFileNameList.isEmpty()) {

            // check if the resource were terminated, meaning the service is no longer operational and we need to close the program
            queues = queue.getQueueList();
            if (queues.size() == 1) {
                if (queues.get(0).equals(summeryFilesIndicatorQueue)) {
                    System.out.println("No queues, manager was terminated");
                    queue.deleteQueue(summeryFilesIndicatorQueue, "Main:");
                    ec2.terminateInstances(null);
                    return;
                }
            }
            // Check if manager crashed, reopen and resend request
            List<Instance> instances = ec2.getInstances("manager");
            if (instances.isEmpty()){
                System.out.println("manager crashed, reactivating the server");
                instance = createManager(queue, ec2);
                if (instance != null) {
                    queue.purgeQueue("workerJobQueue");
                    for (String url:
                            queues) {
                        if (!url.contains("QueueUrlLocalApps") && !url.contains("workerJobQueue") && !url.contains(summeryFilesIndicatorQueue)){
                            System.out.println();
                            System.out.println("url:" + url);
                            System.out.println(urlPrefix+"QueueUrlLocalApps");
                            queue.deleteQueue(url,"Main: ");
                        }
                    }
                    for (String outputfile:
                            outputFileNameList) {
                        if (!outputfile.equals(terminatorOutputFile)){
                            queue.sendMessage(QueueUrlLocalApps, outputfile + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + "dont terminate");
                        }
                        else if (terminationIndicator.equals("terminate") && outputFileNameList.size() == 1){
                            System.out.println("termination case");
                            queue.sendMessage(QueueUrlLocalApps, terminatorOutputFile + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + terminationIndicator);
                        }
                    }
                }
            }

            // check if its time to send the termination file message
            if (terminationIndicator.equals("terminate") && outputFileNameList.size() == 1 && !terminatorOutputFile.equals("")){
                System.out.println("termination case");
                queue.sendMessage(QueueUrlLocalApps, terminatorOutputFile + "@" + inputList.size() + "@" + summeryFilesIndicatorQueue + "@" + terminationIndicator);
                terminatorOutputFile = "";
            }

            // try to get an answer from the manager
            messages = queue.recieveMessage(summeryFilesIndicatorQueue,1,1);
            if (messages != null && !messages.isEmpty()) {
                currMessageName = messages.get(0).getBody().split(delimiter)[0]; // the input file name
                System.out.println(Thread.currentThread().getId() + " Recieved an answer from the manager");

                // Download the ready answer file from the manager and read the contents to a buffer

                S3Object outputObject = s3.downloadObject(currMessageName);
                BufferedReader reader = new BufferedReader(new InputStreamReader(outputObject.getObjectContent()));
                StringBuilder stringBuilder = new StringBuilder();
                String line = "";
                while ((line = reader.readLine()) != null ){
                    stringBuilder.append(line+ "\n");
                }

                System.out.println("Creating the HTML file");
                // create HTML answer file from the buffer containing the content of the answer fie from the manager
                String[] resultsToHTML = stringBuilder.toString().split("\n");
                createHTML(currMessageName,resultsToHTML);

                // delete the message and the outputflename
                System.out.println("size of list before removal of: " + currMessageName + " is: " + outputFileNameList.size());
                outputFileNameList.remove(currMessageName.substring(0,currMessageName.length()-1));
                queue.deleteMessage(summeryFilesIndicatorQueue,messages.get(0));
                System.out.println("size of list after removal of: " + currMessageName + " is: " + outputFileNameList.size());

            }
        }

        System.out.println("Deleting resources");
        queue.deleteQueue(summeryFilesIndicatorQueue, "LocalApp : " + Thread.currentThread().getId());
        System.out.println(Thread.currentThread() + "ending the run");

    }

    private static
    ArrayList<parsedInputObject> parse(String filename) {
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


        int managerinstance = ec2.getInstances("manager").size();
        if (managerinstance == 0){
            return;
        }
        int workerinstances = ec2.getInstances("").size()-managerinstance;
        if ( workerinstances > 13){
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
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject+createAndRunProject;

        // First created instance = worker
        Instance instance = ec2.createInstance(1, 1, userdata).get(0);
        ec2.createTags("worker",instance.getInstanceId());
        ec2.attachTags(instance, "worker");
        System.out.println("Creating Worker: " + instance.getInstanceId());


    }

    private static
    Instance createManager(Queue queue, EC2Object ec2){

        List<Instance> instances = ec2.getInstances("manager");
        if (!instances.isEmpty()){
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
        String userdata = "#!/bin/bash\n" + "cd home/ubuntu/\n" +  buildProject + createAndRunProject;

        // Create Manager and attach tag
        Instance instance = null;
        List<Instance> mangerlist= ec2.createInstance(1, 1, userdata);
        instance = mangerlist.get(0);
        ec2.createTags("manager",instance.getInstanceId());

        // Create permanent queues if they do not exist
        queue.createQueue("QueueUrlLocalApps");
        queue.createQueue("workerJobQueue");

        System.out.println("Created Manager: " + instance.getInstanceId());
        createWorker(ec2);

        return  instance;
    }

    private static void createHTML(String filename, String[] inputRepresentation) throws IOException {
        for (String str : inputRepresentation){
            System.out.println(str);
        }
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

}
