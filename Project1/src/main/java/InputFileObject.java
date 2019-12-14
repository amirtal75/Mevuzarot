import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InputFileObject {

    int id;

    public int getId() {
        return id;
    }

    AtomicInteger inputLines;
    AtomicInteger outputLines;


    AtomicBoolean redAllLines; // finish to read all lines
    AtomicBoolean allWorkersDone; //all the workers finished
    String inputFilename;

    public InputFileObject(int id,String inputFilename){
        this.id = id;
        inputLines = new AtomicInteger(0);
        outputLines = new AtomicInteger(0);;
        redAllLines = new AtomicBoolean(false);
        allWorkersDone = new AtomicBoolean(false);
        this.inputFilename = inputFilename;
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

    public synchronized  void  CheckAndSetAllWorkersDone (){ // check if all workers done and set allWorkersDone accordingly.
        allWorkersDone.compareAndSet(true , (redAllLines.get() & (inputLines.get() == outputLines.get())));
    }

    public String getInputFilename() {
        return inputFilename;
    }

    public void setredAllLinesTrue() {

        redAllLines.set(true);
    }
}
