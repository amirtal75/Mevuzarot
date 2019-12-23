import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InputFileObject {

    int id;

    public int getId() {
        return id;
    }

    AtomicInteger inputLines;
    AtomicInteger outputLines;
    BufferedWriter writer = null;
    private int numberoffilelines= 0;

    AtomicBoolean redAllLines; // finish to read all lines
    AtomicBoolean allWorkersDone; //all the workers finished
    String inputFilename;

    public InputFileObject(int id,String inputFilename, String path, int numberoffilelines){
        this.id = id;
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
        this.numberoffilelines = numberoffilelines;
        try {
            this.writer = new BufferedWriter(new FileWriter(path +inputFilename+"$"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public
    BufferedWriter getWriter() {
        return writer;
    }

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
        allWorkersDone.compareAndSet(false , (redAllLines.get() & (inputLines.get() == outputLines.get())));
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
