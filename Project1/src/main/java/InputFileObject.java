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
    String delimiter = " -@@@@@@@- ";

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
        System.out.println("Created input file object with the ID: " + inputFileID);
    }

    public synchronized BufferedReader getReader() {return reader;}

    public String getInputFileID() {
        return this.inputFileID;
    }

    public void appendToBuffer (String messageFromQueue, String reviewID, String originator) {
        String[] result =  messageFromQueue.split(delimiter );
        System.out.println(originator + "Adding a message with ID: " + result[0] + "\nTo the inputFileObject with the ID: " + inputFileID);
        boolean reviewWasprocessedBefore = iDsOfProcessedReviews.containsValue(reviewID);
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

    public void increaseInputLines() {
        inputLines.getAndIncrement();
    }

    public void increaseOutputLines(String inputFileID, String originator) {
        System.out.println(originator + "increaseOutputLines of the input file:  " + this.inputFileID);
        outputLines.getAndIncrement();
    }

    public void  checkAndSetAllWorkersDone (String originator){ // check if all workers done and set allWorkersDone accordingly.
        System.out.println(originator + "checkAndSetAllWorkersDone of the input file: " + inputFileID);
        allWorkersDone.compareAndSet(false , ((inputLines.get() == numberoffilelines) && (numberoffilelines == outputLines.get())));
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
