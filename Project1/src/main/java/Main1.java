import java.io.*;

public class Main1 {

    public static void main(String[] args) throws IOException {
        String path = "/home/amirtal/IdeaProjects/Localapp/src/main/java/";

        try {
            S3Bucket s3Bucket = new S3Bucket();
            InputStreamReader inputStreamReader = new InputStreamReader(s3Bucket.downloadObject("inputFile1.txtb6e69099-bdcc-4fca-a877-7675f81779d5.txt$").getObjectContent());

            BufferedReader reader = new BufferedReader(inputStreamReader);
            StringBuilder stringBuilder = new StringBuilder();
            String line = "";
            while ((line = reader.readLine()) != null ){
                stringBuilder.append(line+ "\n");
            }
            System.out.println(stringBuilder.toString());

            String[] resultsToHTML = stringBuilder.toString().split("\n");
            createHTML(path,resultsToHTML);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void createHTML(String path, String[] inputRepresentation) throws IOException {
        //String result = inputFileId + "@" + reviewId + "@" + isSarcastic + "@" + reviewText + "@" + reviewEntities + "@" + sentiment;

        String[] colors = {"#97301A", "#F74C28", "#110401", "#6EF443", "#1F6608"};
        StringBuilder html = new StringBuilder("<html>\n" + "<body>");
        for (String str : inputRepresentation) {
            String[] currReviewAttributes = str.split("@");
            //int reviewSentiment = Integer.parseInt(currReviewAttributes[5]);
            int reviewSentiment = Integer.parseInt(currReviewAttributes[4]);
            /*toAdd = "<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + currReviewAttributes[3] + "</h1>" +
                    "<h1>" + currReviewAttributes[4] + " " + reviewSentiment + "</h1>";*/
            html.append("<h1 style=\"background-color:" + colors[reviewSentiment] + ";\">" + currReviewAttributes[3] + "</h1>" +
                    "<h1>" + currReviewAttributes[2] + " " + reviewSentiment + "</h1>");
        }
        html.append("</body>\n" + "</html>");

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("html_output.html"), "utf-8"))) {
            writer.write(html.toString());
        }
    }
}
