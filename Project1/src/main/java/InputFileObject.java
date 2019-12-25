import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
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
    private AtomicInteger numberoffilelines;
    AtomicBoolean redAllLines; // finish to read all lines
    AtomicBoolean allWorkersDone; //all the workers finished
    String inputFilename;
    ConcurrentHashMap<Integer, String> iDsOfProcessedReviews;
    String delimiter = " -@@@@@@@- ";
    String lastReadLine = "";

    public InputFileObject(String inputFilename,int numberoffilelines, S3Object object){
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = new AtomicInteger(numberoffilelines);
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
    public String readLine (){
        try {
            if (lastReadLine != null) {
                lastReadLine = reader.readLine();
                return lastReadLine;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public StringBuffer getBuffer() {return stringBuffer;}

    public  int getNumberoffilelines() {return numberoffilelines.get();}

    public  int getInputLines() {
        return inputLines.get();
    }

    public  int getOutputLines() {
        return outputLines.get();
    }

    public  boolean getRedAllLines() {
        return redAllLines.get();
    }

    public  boolean getAllWorkersDone() {
        return allWorkersDone.get();
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
        allWorkersDone.compareAndSet(false , ((inputLines.get() == numberoffilelines.get()) && (numberoffilelines.get() == outputLines.get())));
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public void setredAllLinesTrue() {
        if (numberoffilelines.get() == inputLines.get()) {
            redAllLines.set(true);
        }
    }

    public String toString(){
        String res = "input lines : " + inputLines.get() + " output lines : " + outputLines.get() +
               "input file name: " + inputFilename + " all lines has been readed " + getRedAllLines() +
                " all workes done : " + allWorkersDone.get();
        return res;
            }
}
