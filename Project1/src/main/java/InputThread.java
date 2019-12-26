import com.amazonaws.services.ec2.model.Instance;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class InputThread implements Runnable {

    String workerJobQueue = "workerJobQueue";
    Queue queue;
    S3Bucket s3;
    InputFileObject currFileObject; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger  numberOfTasks;
    EC2Object ec2;

    public InputThread(InputFileObject currFileObject, AtomicInteger  numberOfTasks) {
        this.queue = new Queue();
        this.s3 = new S3Bucket();
        this.currFileObject = currFileObject;
        this.ec2 = new EC2Object();
        this.numberOfTasks = numberOfTasks;
    }

    public void run() {
        String delimiter = " -@@@@@@@- ";
        String originator = "InputThread: " + Thread.currentThread().getId();
        System.out.println(originator + "Started Running\n");
        String job = "";
        boolean readAllLines = false;
        while (!readAllLines) {


            synchronized (currFileObject) {
                //System.out.println("\n" + originator + " is increasing input lines of: " + currFileObject.getInputFilename() + "from: " + currFileObject.getInputLines());
                job = currFileObject.readLine();
                readAllLines = currFileObject.getRedAllLines();
                //System.out.println("to: " + currFileObject.getInputLines() + " and all lines read status = " + readAllLines + "\n");
            }
            queue.sendMessage(workerJobQueue, job);
            numberOfTasks.incrementAndGet();
        }
        System.out.println(originator+ " finished running");
    }
}
