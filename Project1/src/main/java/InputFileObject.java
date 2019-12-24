import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InputFileObject {

    private String inputFileID;
    AtomicInteger inputLines;
    AtomicInteger outputLines;
    StringBuffer stringBuffer = null;
    BufferedReader reader = null;
    private int numberoffilelines= 0;
    AtomicBoolean redAllLines; // finish to read all lines
    AtomicBoolean allWorkersDone; //all the workers finished
    String inputFilename;
    ConcurrentHashMap<Integer, String> iDsOfProcessedReviews;

    public InputFileObject(String inputFilename,int numberoffilelines, S3Object object){
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = numberoffilelines;
        this.reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));;
        this.stringBuffer = new StringBuffer();
        this.inputFileID  = UUID.randomUUID().toString();
        this.iDsOfProcessedReviews = new ConcurrentHashMap<>();
    }

    public synchronized BufferedReader getReader() {return reader;}

    public String getInputFileID() {
        return this.inputFileID;
    }

    public void appendToBuffer (String messageFromQueue, String reviewID) {
        System.out.println("inside appendToBuffer, message is: " + messageFromQueue);
        System.out.println("inside appendToBuffer, revideID is: " + reviewID);

        boolean reviewWasprocessedBefore = iDsOfProcessedReviews.containsValue(reviewID);
        System.out.println("checking if reviewWasprocessedBefore: " + reviewWasprocessedBefore);
        String toAppend = messageFromQueue + "\n"; //append all the reviews for one inputFile and seperate by "\n"
        if (!reviewWasprocessedBefore) {
            this.stringBuffer.append(toAppend);
            this.iDsOfProcessedReviews.put(outputLines.get(), reviewID);
            this.outputLines.incrementAndGet();
        }
    }

    public StringBuffer getBuffer() {return stringBuffer;}

    public  int getNumberoffilelines() {return numberoffilelines;}

    public  AtomicInteger getInputLines() {
        return inputLines;
    }

    public  AtomicInteger getOutputLines() {
        return outputLines;
    }

    public  AtomicBoolean getRedAllLines() {
        return redAllLines;
    }

    public  AtomicBoolean getAllWorkersDone() {
        return allWorkersDone;
    }

    public synchronized void increaseInputLines() {
        inputLines.getAndIncrement();
    }

    public synchronized void increaseOutputLines() {
        outputLines.getAndIncrement();
    }

    public synchronized void  checkAndSetAllWorkersDone (){ // check if all workers done and set allWorkersDone accordingly.
        allWorkersDone.compareAndSet(false , (redAllLines.get() && (inputLines.get() == outputLines.get())));
    }

    public synchronized String getInputFilename() {
        return inputFilename;
    }

    public synchronized void setredAllLinesTrue() {
        if (numberoffilelines == inputLines.get()) {
            redAllLines.set(true);
        }
    }

    public String toString(){
        String res = "input lines : " + inputLines.get() + " output lines : " + outputLines.get() +
               "input file name: " + inputFilename + " all lines has been readed " + getRedAllLines().get() +
                " all workes done : " + allWorkersDone.get();
        return res;
            }
}
