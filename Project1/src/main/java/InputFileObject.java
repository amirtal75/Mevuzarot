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
    String summeryFilesIndicatorQueue;

    public InputFileObject(String inputFilename, int numberoffilelines, BufferedReader reader, String inputFileID, String summeryFilesIndicatorQueue){
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = new AtomicInteger(numberoffilelines);
        this.reader = reader;;
        this.stringBuffer = new StringBuffer();
        this.inputFileID = inputFileID;
        this.summeryFilesIndicatorQueue = summeryFilesIndicatorQueue;
        this.iDsOfProcessedReviews = new ConcurrentHashMap<>();
        System.out.println("Created input file object with the ID: " + inputFileID);
    }

    public
    String getSummeryFilesIndicatorQueue() {
        return summeryFilesIndicatorQueue;
    }

    public String getInputFileID() {
        return this.inputFileID;
    }

    public void appendToBuffer (String messageFromQueue, String reviewID, String originator) {
        String[] result =  messageFromQueue.split(delimiter );
        System.out.println(originator + "Adding a message with ID: " + result[0] + "\nTo the inputFileObject with the ID: " + inputFileID);
        int numberOfLines = 0;
        boolean reviewWasprocessedBefore = iDsOfProcessedReviews.containsValue(reviewID);
        String toAppend = messageFromQueue + "\n"; //append all the reviews for one inputFile and seperate by "\n"
        if (!reviewWasprocessedBefore) {
            this.stringBuffer.append(toAppend);
            this.iDsOfProcessedReviews.put(outputLines.get(), reviewID);
            numberOfLines = this.outputLines.incrementAndGet();
            allWorkersDone.set(numberOfLines == numberoffilelines.get());
        }
    }
    public String readLine (){
        String line = null;
        int numberOfLines = 0;
        try {
            line = inputFileID + delimiter + reader.readLine();
            numberOfLines = inputLines.incrementAndGet();
            redAllLines.set(numberOfLines == numberoffilelines.get());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
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

    public boolean  checkAndSetAllWorkersDone (String originator){ // check if all workers done and set allWorkersDone accordingly.
        System.out.println(originator + "checkAndSetAllWorkersDone of the input file: " + inputFileID);
        return allWorkersDone.compareAndSet(false , ((inputLines.get() == numberoffilelines.get()) && (numberoffilelines.get() == outputLines.get())));
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
