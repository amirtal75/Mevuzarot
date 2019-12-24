import com.amazonaws.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public
class OutputThread implements Runnable{
    private ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById;
    String completedTasksQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/completedTasksQueue";
    ManagerSuperClass manager;

    public OutputThread(ConcurrentHashMap<Integer, InputFileObject> InputFileObjectById, ManagerSuperClass manager){
        this.InputFileObjectById = InputFileObjectById;
        this.manager = manager;
    }

    public void run() {

        S3Bucket s3= new S3Bucket();
        Queue queue = new Queue();
        List<Message> messagefromCompletedTasksQueue = new ArrayList<Message>();
        String delimiter = " -@@@@@@@- ";
        System.out.println("In Output Thread run: " );

        while(manager.getContinueRunning()){

            messagefromCompletedTasksQueue = queue.recieveMessage(completedTasksQueue, 1, 60); // check about visibility
            InputFileObject currFileObject = null;
            if (!messagefromCompletedTasksQueue.isEmpty()) {

                Message currMessege = messagefromCompletedTasksQueue.get(0);
                String[] resultContent = currMessege.getBody().split(delimiter);
                currFileObject = InputFileObjectById.get(resultContent[0]);
                // String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;

                // The place to check
                if (resultContent[0].equals(currFileObject.getId()) && currFileObject != null) {

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
    }
}


