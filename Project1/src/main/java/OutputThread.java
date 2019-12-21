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
    int i =1;
    public void run() {
        ArrayList<String> arrayList= new ArrayList<>();
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
                i++;
                String[] resultContent = currMessege.getBody().split("@");
                int inputFileId = Integer.parseInt(resultContent[0]);
                //String result = inputFileId + "@" + reviewId + "@" + currIndicator + "@" + reviewText + "@" + reviewEntities +"@"+ sentiment;
                if (stringResultsById.containsKey(inputFileId)) {
                    if (!arrayList.contains(resultContent[1])) {
                        stringResultsById.get(inputFileId).append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
                        arrayList.add(resultContent[1]);
                    }
                }
                    //check again what I sent to the local app
                else {
                    stringResultsById.put(inputFileId, new StringBuilder(currMessege.getBody() + "\n")); // if is absent
                }

                System.out.println("Number of resultbyid after adding: " + stringResultsById.size() + "\nthe cuurent stringBuilder is: " + stringResultsById.get(inputFileId).toString() + "\n");

                InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                currInputFileObj.increaseOutputLines();
                currInputFileObj.CheckAndSetAllWorkersDone();
                System.out.println("All workers done: " + currInputFileObj.getAllWorkersDone().get());
                String inputFilename = currInputFileObj.getInputFilename();

                if (currInputFileObj.getAllWorkersDone().get()) {// if all workers done
                    FileOutputStream outputFile = null;

                    try {
                        String outputName = inputFilename + "$";
                        //added "$" to the name because I dont want exact names for the input file and output file
                        Writer writer = new BufferedWriter(new FileWriter(path + outputName)); //write to the output file
                        System.out.println("string builder content: " + stringResultsById.get(inputFileId).toString() + "\n");
                        writer.write(stringResultsById.get(inputFileId).toString());
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
