import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class InputThread extends ManagerClassesSharedFunctions implements Runnable {

    Queue queue;
    String QueueUrlLocalApps;
    S3Bucket s3;
    String myQueueUrl1; //queue for inputJobs
    String myQueueUrl2; //queue for inputJobs
    InputFileObject currFileObject; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    BufferedReader bufferedReader;

    public InputThread(String queueUrlLocalApps, String myQueueUrl1, String myQueueUrl2, InputFileObject currFileObject, AtomicInteger numberOfTasks) {
        this.queue = new Queue();
        QueueUrlLocalApps = queueUrlLocalApps;
        this.s3 = new S3Bucket();
        this.myQueueUrl1 = myQueueUrl1;
        this.currFileObject = currFileObject;
        this.bufferedReader = currFileObject.getReader();
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.numberOfTasks = numberOfTasks;
        this.myQueueUrl2 = myQueueUrl2;
    }

    public void run() {

        String delimiter = " -@@@@@@@- ";


        String currLine = "";
        String job = "";
        while (!currFileObject.getRedAllLines().get()) {

            System.out.println("inside input thread: " + Thread.currentThread().getId() + "\nworking on the file: " + currFileObject.getInputFilename());

            try {
                currLine = bufferedReader.readLine();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }

            if (currLine != null){

                synchronized (this){
                    job = currFileObject.getId() + delimiter + currLine;
                    queue.sendMessage(myQueueUrl1, job);
                }
                // System.out.println(" Making a job from the current read line: " + currLine);
                // Line content: (obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() +  + obj.getReview().getLink() +"\n"); // added rating******

                currFileObject.increaseInputLines();
                numberOfTasks.incrementAndGet();
                currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                createworker(myQueueUrl1, myQueueUrl2, numberOfTasks.get(), ec2);
            }

        }

        System.out.println("InputThread: " + Thread.currentThread() + " finished running");
    }
}
