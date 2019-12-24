import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public
class UploadThread implements Runnable {
    private ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById;
    protected  String summeryFilesIndicatorQueue = "https://sqs.us-west-2.amazonaws.com/002041186709/summeryFilesIndicatorQueue";
    S3Bucket s3;
    Queue queue;
    String path = "/home/ubuntu/Mevuzarot-master/Project1/src/main/java/";
    ManagerSuperClass manager;

    UploadThread(ConcurrentHashMap<Integer, InputFileObject> inputFileObjectById, ManagerSuperClass manager){
        this.inputFileObjectById = inputFileObjectById;
        this.s3 = new S3Bucket();
        this.queue  = new Queue();
        this.manager = manager;
    }

    public void run() {

        while (manager.getContinueRunning()) {
            InputFileObject currFileObject = null;
            for (int i = 1 ; i < inputFileObjectById.size(); ++i){
                currFileObject = inputFileObjectById.get(i);
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
        }
        System.out.println("Upload Thread: " + Thread.currentThread().getId() + " finished running\n");
    }
}

