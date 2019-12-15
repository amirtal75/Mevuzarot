import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class OutputThread implements Runnable {

    String summeryFilesIndicatorQueue;
    Queue queue;
    String QueueUrlLocalApps;
    ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById;
    List<Message> currMessageQueue = new ArrayList<Message>();
    S3Bucket s3;
    String myQueueUrl2; //queue for outputJobs , should be passed to workers as well
    AmazonEC2 ec2;
    ConcurrentHashMap<Integer, StringBuilder> stringResultsById; // will be passed by manager(the refference) by constructor
    boolean toTerminate;

    public OutputThread(String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuilder> stringResultsById, String QueueUrlLocalApps) throws Exception {
        this.queue = new Queue();
        this.myQueueUrl2 = myQueueUrl2;
        this.QueueUrlLocalApps = QueueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.InputFileObjectById = inputFileObjectById;
        this.stringResultsById = stringResultsById;
        toTerminate = false;
    }

    public void run() {
        System.out.println("In Output Thread: " + Thread.currentThread());
        String currMessageRecieptHandle; // we need to hold a String for deleting the current message each time when we finish
        while (!toTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(myQueueUrl2, 1, 10); // check about visibility
            } catch (Exception e) {
                e.printStackTrace();
            }

            Message currMessege = currMessageQueue.get(0);
            System.out.println("Received message content: " + currMessege.getBody());
            String[] resultContent = currMessege.getBody().split("@");
            int inputFileId = Integer.parseInt(resultContent[0]);
            //String result = inputFileId + "@" + reviewId + "@" + currIndicator + "@" + reviewText + "@" + reviewEntities +"@"+ sentiment;
            if (stringResultsById.containsKey(inputFileId))
                stringResultsById.get(inputFileId).append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
            //check again what I sent to the local app
            else {
                stringResultsById.put(inputFileId, new StringBuilder()); // if is absent
                }

            InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
            currInputFileObj.increaseOutputLines();
            currInputFileObj.CheckAndSetAllWorkersDone();
            System.out.println("All workers done: " + currInputFileObj.getAllWorkersDone().get());
            String inputFilename = currInputFileObj.getInputFilename();

            if (currInputFileObj.getAllWorkersDone().get()) {// if all workers done
                FileOutputStream outputFile = null;

                try {
                    String outputName = inputFilename +"$";
                    //added "$" to the name because I dont want exact names for the input file and output file
                    Writer writer = new BufferedWriter(new FileWriter(outputName)); //write to the output file
                    writer.write(stringResultsById.get(inputFileId).toString());

                    s3.upload(outputName);
                    System.out.println("Upload finised test: " + s3.downloadObject(outputName).getObjectContent().toString());
                    queue.sendMessage(summeryFilesIndicatorQueue, outputName); // outputFilename = key ??????
                }

                catch (IOException e) {
                    e.printStackTrace(); }
                 catch (Exception e) {
                    e.printStackTrace(); }
            }
            try {
                queue.deleteMessage(myQueueUrl2, currMessege);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
