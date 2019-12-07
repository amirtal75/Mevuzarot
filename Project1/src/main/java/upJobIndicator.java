import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

public class upJobIndicator {

    BufferedReader inputFileFromLocalApp ;
    BufferedWriter outputFileToLocalApp;
    int numOfInputFileLines;
    int getNumOfOutputFileLines;
    Boolean inputFileWasRead;

    public upJobIndicator(String key, String bucket, S3Object object) throws IOException {
        this.inputFileFromLocalApp = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        this.outputFileToLocalApp = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(key+"ouput"))));
        this.numOfInputFileLines = 0;
        this.getNumOfOutputFileLines = 0;
        this.inputFileWasRead = false;
    }

    public String readNextLine() throws IOException {
        return this.inputFileFromLocalApp.readLine();
    }

    public BufferedReader getInputFileFromLocalApp() {
        return inputFileFromLocalApp;
    }

    public BufferedWriter getOutputFileToLocalApp() {
        return outputFileToLocalApp;
    }

    public int getNumOfInputFileLines() {
        return numOfInputFileLines;
    }

    public int getGetNumOfOutputFileLines() {
        return getNumOfOutputFileLines;
    }

    public Boolean getInputFileWasRead() {
        return inputFileWasRead;
    }

    public void setInputFileFromLocalApp(BufferedReader inputFileFromLocalApp) {
        this.inputFileFromLocalApp = inputFileFromLocalApp;
    }

    public void setOutputFileToLocalApp(BufferedWriter outputFileToLocalApp) {
        this.outputFileToLocalApp = outputFileToLocalApp;
    }

    public void setNumOfInputFileLines(int numOfInputFileLines) {
        this.numOfInputFileLines = numOfInputFileLines;
    }

    public void setGetNumOfOutputFileLines(int getNumOfOutputFileLines) {
        this.getNumOfOutputFileLines = getNumOfOutputFileLines;
    }

    public void setInputFileWasRead(Boolean inputFileWasRead) {
        this.inputFileWasRead = inputFileWasRead;
    }
}
