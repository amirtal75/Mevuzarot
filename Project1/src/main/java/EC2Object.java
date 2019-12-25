import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import java.util.ArrayList;

import java.util.Base64;
import java.util.List;

public class EC2Object {
    private AmazonEC2 ec2;

    public EC2Object() {
        this.ec2 = AmazonEC2ClientBuilder.defaultClient();
    }

    public AmazonEC2 getEc2() {
        return this.ec2;
    }

    /**
     * @param tagName - Name of tag to create
     * @return if the tag was successfully created
     */
    public boolean createTags(String tagName, String InstanceID){
        if (!tagExists(tagName)) {
            Tag tag = new Tag(tagName, tagName);
            CreateTagsRequest tagsRequest = new CreateTagsRequest()
                    .withResources(InstanceID)
                    .withTags(tag);
            CreateTagsResult result = this.ec2.createTags(tagsRequest);
        }
        return tagExists(tagName);
    }

    /**
     * @param instance - the instance we want to have a tag attached
     * @param tagName - the name of the tag to attch to the instance
     */
    public void attachTags(Instance instance, String tagName) {
        if (!tagExists(tagName)){
            return;
        }

        ArrayList<Tag> tags = new ArrayList<>();
        tags.add(new Tag(tagName,tagName));
        CreateTagsRequest tagsRequest = new CreateTagsRequest()
                .withTags(new Tag(tagName,tagName))
                .withResources(instance.getInstanceId());
        ec2.createTags(tagsRequest);

    }

    /**
     * @param tagName the name of tag to check if exists
     * @return if the tag exists
     */
    public boolean tagExists(String tagName){
        if (tagName.equals("")){
            return false;
        }
        DescribeTagsResult tagsResult= this.ec2.describeTags();
        for (TagDescription tag:
                tagsResult.getTags()) {
            if (tag.getKey().equals(tagName)){
                return true;
            }
        }
        return false;
    }

    /**
     *                 the role will be created only with permission for SQS, S3 & EC2
     * @return if the role creation was succesfull
     */
    /*public boolean createRole(String roleName){
        boolean roleExists = false;
        Region myregion;
        for (Region region:
            ec2.describeRegions().getRegions() ) {

        }
        AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.defaultClient();
        for (Role role:
                iam.listRoles().getRoles()) {
            if (role.getRoleName().equals(roleName) == true) {
                return true;
            }
        }
        String policy = "{\n" +
                "    \"Version\": \"2012-10-17\",\n" +
                "    \"Statement\": [\n" +
                "        {\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Action\": \"s3:*\",\n" +
                "            \"Resource\": \"*\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        CreateRoleRequest roleRequest = new CreateRoleRequest().withRoleName(roleName).withAssumeRolePolicyDocument(policy);

        CreateRoleResult result= iam.createRole(roleRequest);
        AttachRolePolicyRequest request = new AttachRolePolicyRequest().withRoleName(roleName);
        request.withPolicyArn("arn:aws:iam::aws:policy/AmazonS3FullAccess")
                .withPolicyArn("arn:aws:iam::aws:policy/AmazonSQSFullAccess")
                .withPolicyArn("arn:aws:iam::aws:policy/AmazonEC2FullAccess");
        iam.attachRolePolicy(request);

        return result.getRole().getRoleName().equals(roleName);
    }

    /**
     * @param keyName name of the key to create
     * @return returns true if a key with a value of keyname was created, false otherwise
     */
    public boolean createKeyPair(String keyName) {
        boolean hasKey = false;
        for (KeyPairInfo key:
                this.ec2.describeKeyPairs().getKeyPairs()) {
            if (key.getKeyName().equals(keyName)) {
                hasKey = true;
                break;
            }
        }

        if (hasKey == false) {
            CreateKeyPairResult result = this.ec2.createKeyPair(new CreateKeyPairRequest().withKeyName(keyName));
            return result.getKeyPair().getKeyName().equals(keyName);
        }
        return false;
    }

    /**
     * @param min - the min number of instances to create
     * @param max - the max number of instances to create
     * @param userdata - the script to launch for each instance
     * @return the number of instances created
     */
    public ArrayList<Instance> createInstance(int min, int max, String userdata){
        // Convert userData script to base 64
        String encodedUserData = Base64.getEncoder().encodeToString(userdata.getBytes());
        // ami image we created with various installations
        String projectPrivateAmi = "ami-04e1e8f63cdc75cf2";
        // create the project Key Pair
        createKeyPair("projectKey");
        // Create the project IAM Role
        //  createRole("projectRole");
        //Create the request to run
        RunInstancesRequest request = new RunInstancesRequest(projectPrivateAmi, min, max);
        // define instance type
        request.setInstanceType(InstanceType.T2Large.toString());
        // define the script to run in base 64
        request.withUserData(encodedUserData);
        // define the random key pair we created
        request.withKeyName("projectKey");
        // define the iam role
        IamInstanceProfileSpecification specification = new IamInstanceProfileSpecification().withName("projectRole");
        request.withIamInstanceProfile(specification);
        // run the instance with the above defined request
        RunInstancesResult instancesResult = null;
        try{
            instancesResult = this.ec2.runInstances(request);
        } catch (Exception e){
            System.out.println(e.getMessage());
            return new ArrayList<Instance>();
        }

        return new ArrayList<Instance> ( instancesResult.getReservation().getInstances());
    }


    /**
     * @param instances - list of instances to terminate.
     *                  if the list is null, all instances will be terminated
     * @return Number of instacnes terminated
     */
    public int terminateInstances(ArrayList<Instance> instances) {
        if (instances == null) {
            instances = getInstances("");
        }

        if (instances.size() == 0){
            return 0;
        }

        ArrayList<String> instancesToTerminate = new ArrayList<>();
        for (Instance instance:
                instances) {
            instancesToTerminate.add(instance.getInstanceId());
        }
        DeleteTagsRequest deleteTagsRequest = new DeleteTagsRequest()
                .withResources(instancesToTerminate)
                .withTags(new Tag("manager","manager"));

        ec2.deleteTags(deleteTagsRequest);
        TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(instancesToTerminate);
        if (!instances.isEmpty()) {
            TerminateInstancesResult result = this.ec2.terminateInstances(terminateRequest);
            return result.getTerminatingInstances().size();
        }
        return 0;

    }

    /**
     * @param tagName - can be empty string or an an existing tag name
     * @return - return a list of instances that has tagName attached to them.
     *          If the the tag is empty string, then return all instances.
     */
    public ArrayList<Instance> getInstances(String tagName){

        DescribeInstancesRequest request = new DescribeInstancesRequest();
        boolean notdone = true;
        DescribeInstancesResult response= null;
        ArrayList<Instance> instancesResult= new ArrayList<>();
        while(notdone) {
            try {
                response = this.ec2.describeInstances(request);

            } catch (Exception e){
                try {
                    System.out.println("Ec2 describe Exception");
                    Thread.sleep(1000);
                    return getInstances(tagName);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            List<Reservation> reservations = response.getReservations();

            for(Reservation reservation :
                    reservations) {
                List<Instance> instances = reservation.getInstances();
                for (Instance instance:
                        instances ) {
                    Tag tag = new Tag(tagName,tagName);
                    Boolean run = instance.getState().getName().equals("running");
                    Boolean pend = instance.getState().getName().equals("pending");
                    if ( (tagName.equals("") || tagName == null) && ( run || pend )){
                        instancesResult.add(instance);
                    }
                    else if (instance.getTags().contains(tag) && (run || pend)){
                        instancesResult.add(instance);
                    }
                }
            }
            request.setNextToken(response.getNextToken());

            if(response.getNextToken() == null) {
                notdone = false;
            }

        }
        return  instancesResult;
    }

    public void stopInstance(String instanceID){
        this.ec2.stopInstances(new StopInstancesRequest().withInstanceIds(instanceID));
    }
}
