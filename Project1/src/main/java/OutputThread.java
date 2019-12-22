import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OutputThread implements Runnable {

    String summeryFilesIndicatorQueue;
    Queue queue;
    ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById;
    List<Message> currMessageQueue = new ArrayList<Message>();
    S3Bucket s3;
    String myQueueUrl2; //queue for outputJobs , should be passed to workers as well
    AmazonEC2 ec2;
    ConcurrentHashMap<Integer, StringBuilder> stringResultsById; // will be passed by manager(the refference) by constructor
    boolean toTerminate;
    AtomicInteger numberOfCompletedTasks;

    public
    OutputThread(String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuilder> stringResultsById, String summeryFilesIndicatorQueue, AtomicInteger numberOfCompletedTasks) throws Exception {
        this.queue = new Queue();
        this.myQueueUrl2 = myQueueUrl2;
        this.summeryFilesIndicatorQueue = summeryFilesIndicatorQueue;
        this.s3 = new S3Bucket();
        this.InputFileObjectById = inputFileObjectById;
        this.stringResultsById = stringResultsById;
        toTerminate = false;
        this.numberOfCompletedTasks = numberOfCompletedTasks;
    }

    public
    void run() {
        String delimiter = " -@@@@@@@- ";
        ArrayList<String> completedreviewIDlist = new ArrayList<>();
        System.out.println("In Output Thread: " + Thread.currentThread());
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        int numberOftasksworkedbythisOutputThread = 0;
        while (!toTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(myQueueUrl2, 1, 60); // check about visibility
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!currMessageQueue.isEmpty()) {
                ++numberOftasksworkedbythisOutputThread;
                System.out.println(" Num of tasks perform by this output thread: " + Thread.currentThread().getId() + " is: " + numberOftasksworkedbythisOutputThread);
                Message currMessege = currMessageQueue.get(0);
                // System.out.println("Received message content: " + currMessege.getBody());
                String[] resultContent = currMessege.getBody().split(delimiter);
                int inputFileId = Integer.parseInt(resultContent[0]);
                //String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;
                InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                if (!completedreviewIDlist.contains(resultContent[1])) {
                    if (stringResultsById.containsKey(inputFileId)) {
                        StringBuilder builder = new StringBuilder(stringResultsById.get(inputFileId).toString());
                        builder.append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
                        stringResultsById.replace(inputFileId,builder);
                        completedreviewIDlist.add(resultContent[1]);
                        currInputFileObj.increaseOutputLines();
                    }
                    //check again what I sent to the local app
                    else {
                        stringResultsById.put(inputFileId, new StringBuilder(currMessege.getBody() + "\n")); // if is absent
                        currInputFileObj.increaseOutputLines();
                    }
                    numberOfCompletedTasks.incrementAndGet();
                }

                try {
                    queue.deleteMessage(myQueueUrl2, currMessege);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //System.out.println("All workers done: " + currInputFileObj.getAllWorkersDone().get());
                String inputFilename = currInputFileObj.getInputFilename();

            }
        }
        }
    }


