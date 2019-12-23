import com.amazonaws.services.sqs.model.Message;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OutputThread implements Runnable {

    InputFileObject currFileObject;
    String myQueueUrl2; //queue for outputJobs , should be passed to workers as well
    boolean toTerminate;
    AtomicInteger numberOfCompletedTasks;
    String summeryFilesIndicatorQueue;

    public OutputThread(String myQueueUrl2, String summeryFilesIndicatorQueue, InputFileObject currFileObject, AtomicInteger numberOfCompletedTasks){
        this.myQueueUrl2 = myQueueUrl2;
        this.currFileObject = currFileObject;
        toTerminate = false;
        this.numberOfCompletedTasks = numberOfCompletedTasks;
        this.summeryFilesIndicatorQueue = summeryFilesIndicatorQueue;
    }

    public
    void run() {
        S3Bucket s3= new S3Bucket();
        Queue queue = new Queue();
        List<Message> messagefromCompletedTasksQueue = new ArrayList<Message>();
        String delimiter = " -@@@@@@@- ";
        System.out.println("In Output Thread: " );
        String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";

        while (!currFileObject.getAllWorkersDone().get()) {

            System.out.println("In Output Thread: " + Thread.currentThread() + " The input file worked on in this task: " + currFileObject.getInputFilename());

            messagefromCompletedTasksQueue = queue.recieveMessage(myQueueUrl2, 1, 60); // check about visibility
            if (!messagefromCompletedTasksQueue.isEmpty()) {

                Message currMessege = messagefromCompletedTasksQueue.get(0);
                String[] resultContent = currMessege.getBody().split(delimiter);
                // String result = inputFileId + delimiter + reviewId + delimiter + currIndicator + delimiter + reviewText + delimiter + reviewEntities +delimiter+ sentiment;

                // The place to check
                currFileObject.appendToBuffer(currMessege.getBody(),resultContent[1] );
                numberOfCompletedTasks.incrementAndGet();
                queue.deleteMessage(myQueueUrl2, currMessege);
            }
        }

        synchronized (this){
            if (currFileObject.getAllWorkersDone().get()){
                try {
                    String outputName = currFileObject.getInputFilename() + "$";
                    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + outputName));
                    bufferedWriter.write(currFileObject.getBuffer().toString());
                    bufferedWriter.flush();
                    s3.upload(path, outputName);
                    queue.sendMessage(summeryFilesIndicatorQueue, outputName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        System.out.println("Output Thread: " + Thread.currentThread().getId() + " finished running\n");
    }
}


