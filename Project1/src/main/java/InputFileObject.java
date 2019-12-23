import java.io.BufferedReader;
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

    public InputFileObject(int id,String inputFilename, String path, int numberoffilelines, BufferedReader reader){
        this.id = new AtomicInteger(id);
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = numberoffilelines;
        this.reader = reader;
        this.stringBuffer = new StringBuffer();
    }

    public BufferedReader getReader() {return reader;}

    public int getId() {
        return this.id.get();
    }

    public void appendToBuffer (String messageFromQueue, String reviewID) {
        boolean reviewWasprocessedBefore = iDsOfProcessedReviews.containsValue(reviewID);

        if (!reviewWasprocessedBefore) {
            this.stringBuffer.append(messageFromQueue + "\n"); //append all the reviews for one inputFile and seperate by "\n"
            this.iDsOfProcessedReviews.put(outputLines.get(), reviewID);
            outputLines.incrementAndGet();
        }
    }

    public StringBuffer getBuffer() {return stringBuffer;}

    public int getNumberoffilelines() {return numberoffilelines;}

    public AtomicInteger getInputLines() {
        return inputLines;
    }

    public AtomicInteger getOutputLines() {
        return outputLines;
    }

    public AtomicBoolean getRedAllLines() {
        return redAllLines;
    }

    public AtomicBoolean getAllWorkersDone() {
        return allWorkersDone;
    }

    public AtomicBoolean AllWorkersDone() {
        return allWorkersDone;
    }

    public void increaseInputLines() {
        inputLines.getAndIncrement();
    }

    public void increaseOutputLines() {
        outputLines.getAndIncrement();
    }

    public void  CheckAndSetAllWorkersDone (){ // check if all workers done and set allWorkersDone accordingly.
        allWorkersDone.compareAndSet(false , (redAllLines.get() && (inputLines.get() == outputLines.get())));
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public void setredAllLinesTrue() {
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
