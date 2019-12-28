import com.amazonaws.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OutputThread implements Runnable {

    InputFileObject currFileObject;
    String completedTasksQueue; //queue for outputJobs , should be passed to workers as well
    AtomicInteger numberOfCompletedTasks;
    String originator;

    public OutputThread(InputFileObject currFileObject, AtomicInteger numberOfCompletedTasks){
        synchronized (currFileObject){
            this.completedTasksQueue = currFileObject.getInputFileID();
        }
        this.currFileObject = currFileObject;
        this.numberOfCompletedTasks = numberOfCompletedTasks;
    }

    public
    void run() {
        originator = "OutputThread: " + Thread.currentThread().getId();
        Queue queue = new Queue();
        List<Message> messagefromCompletedTasksQueue = new ArrayList<Message>();
        String delimiter = " -@@@@@@@- ";
        //System.out.println("In Output Thread: " + Thread.currentThread());
        boolean queueExist = true;
        boolean wroteAllLines = false;
        while (!wroteAllLines && queueExist) {

            messagefromCompletedTasksQueue = queue.recieveMessage(completedTasksQueue, 1, 60); // check about visibility
            if (messagefromCompletedTasksQueue != null && !messagefromCompletedTasksQueue.isEmpty()) {

                Message currMessege = messagefromCompletedTasksQueue.get(0);
                String[] resultContent = currMessege.getBody().split(delimiter);
                synchronized (currFileObject) {
                    System.out.println("\n" + originator + " is increasing output lines of teh file object with the detials: " + currFileObject+ "\n" + " from: " + currFileObject.getOutputLines());
                    currFileObject.appendToBuffer(currMessege.getBody(), resultContent[1],originator);
                    queueExist = false;
                    for (String url:
                            queue.getQueueList()) {
                        if (url.contains(completedTasksQueue)){
                            queueExist = true;
                        }
                    }
                    wroteAllLines = currFileObject.getAllWorkersDone();
                    System.out.println("to: " + currFileObject.getOutputLines() + " and all lines read status = " + wroteAllLines + "\n");
                }
                queue.deleteMessage(completedTasksQueue, currMessege);
                numberOfCompletedTasks.incrementAndGet();
            }
        }
        System.out.println("Output Thread: " + Thread.currentThread().getId() + " finished running\n");
    }
}
