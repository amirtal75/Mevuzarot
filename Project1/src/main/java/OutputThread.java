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
    boolean toTerminate;

    public OutputThread(String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, String QueueUrlLocalApps, String summeryFilesIndicatorQueue) throws Exception {
        this.queue = new Queue();
        this.myQueueUrl2 = myQueueUrl2;
        this.QueueUrlLocalApps = QueueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.InputFileObjectById = inputFileObjectById;
        this.summeryFilesIndicatorQueue = summeryFilesIndicatorQueue;
        toTerminate = false;
    }
    int i =1;
    public void run() {
        BufferedWriter writer = null;
        ArrayList<String> completedreviewIDlist= new ArrayList<>();
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        System.out.println("In Output Thread: " + Thread.currentThread());
        while (!toTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(myQueueUrl2, 1, 10); // check about visibility
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!currMessageQueue.isEmpty()){
                Message currMessege = currMessageQueue.get(0);
                System.out.println("Received message number " + i + " content: " + currMessege.getBody());
                ++i;
                String[] resultContent = currMessege.getBody().split("@");
                int inputFileId = Integer.parseInt(resultContent[0]);
                InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                //String result = inputFileId + "@" + reviewId + "@" + currIndicator + "@" + reviewText + "@" + reviewEntities +"@"+ sentiment;

                writer = currInputFileObj.getWriter();
                if (!completedreviewIDlist.contains(resultContent[1])) {
                    try {
                        writer.write(currMessege.getBody() + "\n");
                        System.out.println("increasing output from: " + currInputFileObj.getOutputLines());
                        currInputFileObj.increaseOutputLines();
                        System.out.println("after increase number og output lines: " + currInputFileObj.getOutputLines());

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //stringResultsById.get(inputFileId).append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
                    completedreviewIDlist.add(resultContent[1]);
                }
                
                    //check again what I sent to the local app
                currInputFileObj.CheckAndSetAllWorkersDone();
                System.out.println("All workers done: " + currInputFileObj.getAllWorkersDone().get());
                String inputFilename = currInputFileObj.getInputFilename();

                if (currInputFileObj.getAllWorkersDone().get()) {// if all workers done
                    try {
                        String outputName = inputFilename + "$";
                        //added "$" to the name because I dont want exact names for the input file and output file
                        writer.flush();

                        s3.upload(path, outputName);
                        System.out.println("sending finished output file to local app" + summeryFilesIndicatorQueue);
                        queue.sendMessage(summeryFilesIndicatorQueue, outputName); // outputFilename = key ??????
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    queue.deleteMessage(myQueueUrl2, currMessege);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
