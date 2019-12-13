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
import java.util.Base64;
import java.util.List;

public class Main {




    public static void main(String[] args) throws Exception {

        Queue queue = new Queue();
        String summeryFilesIndicatorQueue = queue.createQueue();
        String QueueUrlLocalApps = queue.createQueue();
        EC2Object ec2 = new EC2Object();
        String bucketName = "s3://akia23oa26bbqmk5bu2fabcassigment1/";
        String fileToDownload = "maven.jar";
        String downloadCommand = "aws s3 cp " + bucketName +fileToDownload + " " + fileToDownload +"\n";
        String managerArgs = " " + QueueUrlLocalApps + " " + summeryFilesIndicatorQueue + "\n";
        String userdata = "#!/bin/bash\n" + downloadCommand + "java -jar " + fileToDownload + managerArgs;
        System.out.println(userdata);
        ec2.terminateInstances(null);
        ec2.createInstance(1,1,userdata);
    }
}
