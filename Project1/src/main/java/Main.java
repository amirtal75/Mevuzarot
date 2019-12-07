import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.opsworks.model.CreateInstanceRequest;
import com.amazonaws.services.opsworks.model.DeleteInstanceRequest;
import com.google.gson.Gson;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    String loacalAppQueueUrl;



    public static void main(String[] args) throws Exception {

        String path = "/home/amirtal/IdeaProjects/Project1/src/main/java/";
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());;
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        /*Queue queue = new Queue(credentialsProvider);
        S3Bucket s3 = new S3Bucket("assignment1", credentialsProvider);
        String queueUrl = queue.createQueue();
        LocalApp test = new LocalApp(queueUrl, "inputFile1.txt", credentialsProvider);
        createInstances(1,1,s3.bucketName, credentialsProvider );

        Manager manager = new Manager();

        //manager.work( queueUrl);
    */
        S3Bucket s3 = new S3Bucket("assignment1", credentialsProvider);
        ArrayList<Instance> Ids = getInstances(ec2);
        if (!hasManager(Ids, ec2)){

            createInstance(Ids, ec2,5,5, "");
            Ids = getInstances(ec2);
            createTags(ec2, Ids.get(0).getInstanceId(), new Tag("manager", "manager"));
        }


        terminateInstances(Ids, ec2);

    }

    private static boolean hasManager(ArrayList<Instance> Ids, AmazonEC2 ec2){
        for (Instance instance:
                Ids) {
            for (Tag tag:
                    instance.getTags()) {
                if (tag.getKey().equals("manager")){
                    return true;
                }
            }
        }
        return false;
    }

    private static void createTags(AmazonEC2 ec2, String instanceId, Tag tag){
        System.out.println(instanceId);
        CreateTagsRequest request = new CreateTagsRequest().withResources(instanceId).withTags(tag);
        CreateTagsResult response = ec2.createTags(request);
    }

    private static void createInstance(ArrayList<Instance> Ids, AmazonEC2 ec2, int min, int max, String userdata){
        RunInstancesRequest request = new RunInstancesRequest("ami-0c5204531f799e0c6", min, max);
        request.setInstanceType(InstanceType.T1Micro.toString());
        request.withUserData(userdata);
        RunInstancesResult instancesResult = ec2.runInstances(request);
    }

    private static void terminateInstances(ArrayList<Instance> Ids, AmazonEC2 ec2) {
        try {
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            TerminateInstancesRequest terminateRequest;
            for (Instance instance:
                 Ids) {
                terminateRequest = new TerminateInstancesRequest();
                terminateRequest.withInstanceIds(instance.getInstanceId());
                ec2.terminateInstances(terminateRequest);
            }



        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
    }

    private static ArrayList<Instance> getInstances(AmazonEC2 ec2){
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        boolean notdone = true;

        ArrayList<Instance> Ids= new ArrayList<>();
        while(notdone) {
            DescribeInstancesResult response = ec2.describeInstances(request);
            List<Reservation> reservations = response.getReservations();
            if (reservations.isEmpty()){
                notdone = false;
            }
            else{
                for(Reservation reservation : reservations) {
                    //System.out.println(reservations.size());
                    List<Instance> instances = reservation.getInstances();
                    if (instances.isEmpty()){
                        notdone = false;
                    }
                    else{
                        //System.out.println(instances.size());
                        Ids.addAll(instances);
                    }
                }
            }


            request.setNextToken(response.getNextToken());

            if(response.getNextToken() == null) {
                notdone = false;
            }
        }
        return  Ids;
    }

    private static ArrayList<parsedInputObject> parse(String filename,BookList inputRepresentation) {

        ArrayList<parsedInputObject> inputArray = new ArrayList<parsedInputObject>();
        Gson gson = new Gson();
        BufferedReader reader;
        ArrayList<Review> reviews;
        try {
            reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                Book dataholder = gson.fromJson(line, Book.class);
                inputRepresentation.add(dataholder);

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
}
