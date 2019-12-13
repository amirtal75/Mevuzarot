import java.io.*;

public class Main1 {

    public static void main(String[] args) throws IOException {
        //String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;
        String[] inputRepresentation = {"1234","1111","false","the book is amazing , it was a pleasure to read it",
        "bbbbbbb","3" };

            String inputFileId;
            String reviewId;
            String isSarcastic;
            String reviewText;
            String reviewEntities;
            int reviewSentiment;

            String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
            StringBuilder html = new StringBuilder("<html>\n" + "<body>");
            //for (String str : inputRepresentation) {
                //String[] currReviewAttributes = str.split("@");
                inputFileId = inputRepresentation[0];
                reviewId = inputRepresentation[1]; // do we need to write it on the html file?
                isSarcastic = inputRepresentation[2];
                reviewText = inputRepresentation[3];
                reviewEntities = inputRepresentation[4];
                reviewSentiment = Integer.parseInt(inputRepresentation[5]);
            html.append("<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + reviewText + "</h1>" +
                "<h1>" + reviewEntities + " " + isSarcastic + "</h1>");
            html.append("</body>\n" + "</html>");

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("html_output.html"), "utf-8"))) {
                writer.write(html.toString());
        }
    }
}
