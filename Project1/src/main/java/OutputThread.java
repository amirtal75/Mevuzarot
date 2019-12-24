import com.amazonaws.services.sqs.model.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public
class OutputThread implements Runnable{
    private ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById;
    String completedTasksQueue = "completedTasksQueue";
    ManagerSuperClass manager;

    public OutputThread(ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById, ManagerSuperClass manager){
        this.InputFileObjectById = InputFileObjectById;
        this.manager = manager;
    }

    public void run() {
        String identity = "OutputThread: " + Thread.currentThread().getId() + "\n";
        System.out.println(identity+ " started running");
        S3Bucket s3= new S3Bucket();
        Queue queue = new Queue();
        List<Message> messagefromCompletedTasksQueue = new ArrayList<Message>();
        String delimiter = " -@@@@@@@- ";

        while(manager.getContinueRunning()){
            System.out.println();
            messagefromCompletedTasksQueue = queue.recieveMessage(completedTasksQueue, 1, 60); // check about visibility
            System.out.println(identity + "message received: " +messagefromCompletedTasksQueue.isEmpty());
            System.out.println(identity + "message content:\n "+ messagefromCompletedTasksQueue.get(0).getBody());
            InputFileObject currFileObject = null;
            if (!messagefromCompletedTasksQueue.isEmpty()) {

                Message currMessege = messagefromCompletedTasksQueue.get(0);
                String[] resultContent = currMessege.getBody().split(delimiter);
                currFileObject = InputFileObjectById.get(resultContent[0]);
                // String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;

                // The place to check
                if (currFileObject != null && resultContent[0].equals(currFileObject.getId())) {

                    System.out.println("message from worker: ");
                    for (String str: resultContent){
                        System.out.println(str);
                    }
                    System.out.println("In Output Thread: " + Thread.currentThread() + " The input file worked on in this task: " + currFileObject.getInputFilename());
                    currFileObject.appendToBuffer(currMessege.getBody(), resultContent[1]);
                    manager.setNumberOfCompletedTasks(manager.getNumberOfCompletedTasks()+1);
                    queue.deleteMessage(completedTasksQueue, currMessege);
                }
            }
        }
        System.out.println("OutputThread: " + Thread.currentThread() + " finished running");
    }
}


