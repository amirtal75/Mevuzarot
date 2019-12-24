import com.amazonaws.services.sqs.model.Message;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OutputThread implements Runnable {

    InputFileObject currFileObject;
    String completedTasksQueue; //queue for outputJobs , should be passed to workers as well
    AtomicInteger numberOfCompletedTasks;
    String originator;

    public OutputThread(InputFileObject currFileObject, AtomicInteger numberOfCompletedTasks){
        this.completedTasksQueue = currFileObject.getInputFileID();
        this.currFileObject = currFileObject;
        this.numberOfCompletedTasks = numberOfCompletedTasks;
        originator = "Thread: " + Thread.currentThread().getId() + "is initiating the following task for input file:" + currFileObject.getInputFileID()+ "\n";
    }

    public
    void run() {
        Queue queue = new Queue();
        List<Message> messagefromCompletedTasksQueue = new ArrayList<Message>();
        String delimiter = " -@@@@@@@- ";
        System.out.println("In Output Thread: " + Thread.currentThread() + " The input file worked on in this task: " + currFileObject.getInputFilename());

        while (!currFileObject.getAllWorkersDone().get()) {

            messagefromCompletedTasksQueue = queue.recieveMessage(completedTasksQueue, 1, 60); // check about visibility
            if (!messagefromCompletedTasksQueue.isEmpty()) {

                Message currMessege = messagefromCompletedTasksQueue.get(0);
                String[] resultContent = currMessege.getBody().split(delimiter);
                // String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;
                synchronized (this) {
                    currFileObject.appendToBuffer(currMessege.getBody(), resultContent[1],originator);
                    currFileObject.increaseOutputLines(currFileObject.getInputFileID(),originator);
                    numberOfCompletedTasks.incrementAndGet();
                    currFileObject.checkAndSetAllWorkersDone(originator);
                    queue.deleteMessage(completedTasksQueue, currMessege);
                }
            }
        }
        System.out.println("Output Thread: " + Thread.currentThread().getId() + " finished running\n");
    }
}

