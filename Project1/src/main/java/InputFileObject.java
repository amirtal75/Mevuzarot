import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InputFileObject {

    AtomicInteger id;
    AtomicInteger inputLines;
    AtomicInteger outputLines;
    StringBuffer stringBuffer = null;
    BufferedReader reader = null;
    private int numberoffilelines= 0;
    AtomicBoolean redAllLines; // finish to read all lines
    AtomicBoolean allWorkersDone; //all the workers finished
    String inputFilename;
    ConcurrentHashMap<Integer, String> iDsOfProcessedReviews;

    public InputFileObject(int id,String inputFilename, String path, int numberoffilelines, S3Object object){
        this.id = new AtomicInteger(id);
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = numberoffilelines;
        this.reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));;
        this.stringBuffer = new StringBuffer();
    }

    public synchronized BufferedReader getReader() {return reader;}

    public synchronized int getId() {
        return this.id.get();
    }

    public synchronized void appendToBuffer (String messageFromQueue, String reviewID) {
        boolean reviewWasprocessedBefore = iDsOfProcessedReviews.containsValue(reviewID);
        System.out.println("checking if reviewWasprocessedBefore: " + reviewWasprocessedBefore);
        String toAppend = messageFromQueue + "\n"; //append all the reviews for one inputFile and seperate by "\n"
        if (!reviewWasprocessedBefore) {
            this.stringBuffer.append(toAppend);
            this.iDsOfProcessedReviews.put(outputLines.get(), reviewID);
            this.outputLines.incrementAndGet();
        }
    }

    public synchronized StringBuffer getBuffer() {return stringBuffer;}

    public synchronized int getNumberoffilelines() {return numberoffilelines;}

    public synchronized AtomicInteger getInputLines() {
        return inputLines;
    }

    public synchronized AtomicInteger getOutputLines() {
        return outputLines;
    }

    public synchronized AtomicBoolean getRedAllLines() {
        return redAllLines;
    }

    public synchronized AtomicBoolean getAllWorkersDone() {
        return allWorkersDone;
    }

    public synchronized AtomicBoolean AllWorkersDone() {
        return allWorkersDone;
    }

    public synchronized void increaseInputLines() {
        inputLines.getAndIncrement();
    }

    public synchronized void increaseOutputLines() {
        outputLines.getAndIncrement();
    }

    public synchronized void  CheckAndSetAllWorkersDone (){ // check if all workers done and set allWorkersDone accordingly.
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
