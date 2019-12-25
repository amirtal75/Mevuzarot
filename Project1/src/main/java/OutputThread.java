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
    String completedTasksQueue; //queue for outputJobs , should be passed to workers as well
    AmazonEC2 ec2;
    ConcurrentHashMap<Integer, StringBuffer> stringResultsById; // will be passed by manager(the refference) by constructor
    boolean toTerminate;
    AtomicInteger numberOfCompletedTasks;

    public
    OutputThread(String completedTasksQueue, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuffer> stringResultsById, String summeryFilesIndicatorQueue, AtomicInteger numberOfCompletedTasks){
        this.queue = new Queue();
        this.completedTasksQueue = completedTasksQueue;
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
        //System.out.println("In Output Thread: " + Thread.currentThread());
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
        int numberOftasksworkedbythisOutputThread = 0;
        while (!toTerminate) {
            try {
                currMessageQueue = queue.recieveMessage(completedTasksQueue, 1, 60); // check about visibility
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!currMessageQueue.isEmpty()) {
                ++numberOftasksworkedbythisOutputThread;
                Message currMessege = currMessageQueue.get(0);
                // //System.out.println("Received message content: " + currMessege.getBody());
                String[] resultContent = currMessege.getBody().split(delimiter);
                int inputFileId = Integer.parseInt(resultContent[0]);
                //String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;
                InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                synchronized (this){

                    //System.out.println("The outputThread: " + Thread.currentThread().getId() + "is about to perform a change to the following input file object:");
                    //System.out.println(currInputFileObj);

                    if ( currInputFileObj != null && !completedreviewIDlist.contains(resultContent[1])) {
                        if (stringResultsById.containsKey(inputFileId)) {
                            StringBuffer builder = stringResultsById.get(inputFileId);
                            builder.append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
                            completedreviewIDlist.add(resultContent[1]);
                        }
                        //check again what I sent to the local app
                        else {
                            stringResultsById.put(inputFileId, new StringBuffer(currMessege.getBody() + "\n")); // if is absent
                        }
                        currInputFileObj.increaseOutputLines();
                        numberOfCompletedTasks.incrementAndGet();
                        queue.deleteMessage(completedTasksQueue, currMessege);
                    }

                    //System.out.println("The outputThread: " + Thread.currentThread().getId() + "completed the change to the following input file object:");
                    //System.out.println(currInputFileObj);
                }

            }
        }
    }
}


