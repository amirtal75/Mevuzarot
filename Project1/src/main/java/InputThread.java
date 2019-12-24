import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;


public class InputThread implements Runnable {

    String workerJobQueue = "workerJobQueue";
    Queue queue;
    S3Bucket s3;
    InputFileObject currFileObject; // all the FileObject by their id . shared between inputThreas,OutputThread,workers.
    AtomicInteger numberOfTasks = new AtomicInteger(0);
    EC2Object ec2;
    boolean toTerminate;
    BufferedReader bufferedReader;
    ManagerSuperClass manager;

    public InputThread(InputFileObject currFileObject, ManagerSuperClass manager) {
        this.queue = new Queue();
        this.s3 = new S3Bucket();
        this.currFileObject = currFileObject;
        this.bufferedReader = currFileObject.getReader();
        this.ec2 = new EC2Object();
        toTerminate = false;
        this.manager = manager;
    }

    public void run() {

        String delimiter = " -@@@@@@@- ";


        String currLine = "";
        String job = "";
        while (!currFileObject.getRedAllLines().get()) {

            //System.out.println("inside input thread: " + Thread.currentThread().getId() + "\nworking on the file: " + currFileObject.getInputFilename());

            try {
                currLine = bufferedReader.readLine();
            } catch (IOException e) {
                //System.out.println(e.getMessage());
            }

            if (currLine != null){

                synchronized (this){
                    job = currFileObject.getId() + delimiter + currLine;
                    queue.sendMessage(workerJobQueue, job);
                }
                // //System.out.println(" Making a job from the current read line: " + currLine);
                // Line content: (obj.getReview().getId() + delimiter + obj.getReview().getText() + delimiter + obj.getReview().getRating() +  + obj.getReview().getLink() +"\n"); // added rating******

                currFileObject.increaseInputLines();
                numberOfTasks.incrementAndGet();
                currFileObject.setredAllLinesTrue(); // we've finished to read all lines of the input file
                manager.createworker(ec2, queue);
            }

        }

        System.out.println("InputThread: " + Thread.currentThread() + " finished running");
    }
}
