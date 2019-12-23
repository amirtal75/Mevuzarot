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
    ConcurrentHashMap<Integer, StringBuffer> stringResultsById; // will be passed by manager(the refference) by constructor
    boolean toTerminate;
    AtomicInteger numberOfCompletedTasks;

    public
    OutputThread(String myQueueUrl2, ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ConcurrentHashMap<Integer, StringBuffer> stringResultsById, String summeryFilesIndicatorQueue, AtomicInteger numberOfCompletedTasks) throws Exception {
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

                synchronized (this){

                    InputFileObject currInputFileObj = InputFileObjectById.get(inputFileId);
                    String filename = currInputFileObj.getInputFilename();
                    System.out.println("In Output Thread: The input file worked on in this task: " + filename);

                    if (!completedreviewIDlist.contains(resultContent[1])) {
                        System.out.println("In Output Thread: The current number of increaseOutputLines is: " + currInputFileObj.getOutputLines());
                        System.out.println("In Output Thread: The current number of completed tasks is: " + numberOfCompletedTasks);
                        if (stringResultsById.containsKey(inputFileId)) {
                            System.out.println("In Output Thread: Working on existing string buffer");
                            StringBuffer builder = stringResultsById.get(inputFileId);
                            builder.append(currMessege.getBody() + "\n"); //append all the reviews for one inputFile and seperate by "\n"
                            completedreviewIDlist.add(resultContent[1]);

                            System.out.println("In Output Thread: added a line to the existing builder of the file: " + filename);

                            currInputFileObj.increaseOutputLines();
                        }

                        //check again what I sent to the local app
                        else {
                            stringResultsById.put(inputFileId, new StringBuffer(currMessege.getBody() + "\n")); // if is absent
                            currInputFileObj.increaseOutputLines();
                        }
                        numberOfCompletedTasks.incrementAndGet();
                        System.out.println("In Output Thread: Task completed and the current number of increaseOutputLines is: " + currInputFileObj.getOutputLines());
                        System.out.println("In Output Thread: Task completed and the current number of completed tasks is: " + numberOfCompletedTasks);
                    }

                    try {
                        queue.deleteMessage(myQueueUrl2, currMessege);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        System.out.println("Output Thread: " + Thread.currentThread().getId() + " finished running\n");
        }
    }


