public class parsedInputObject {

    String title;
    Review review;
    int sentiment;

    public parsedInputObject(String title, Review review) {
        this.title = title;
        this.review = review;
        this.sentiment = -1;
    }

    public String getTitle() {
        return title;
    }

    public Review getReview() {
        return review;
    }

    public int getSentiment() {
        return sentiment;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setReview(Review review) {
        this.review = review;
    }

    public void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    @Override
    public String toString() {
        return "parsedInputObject{" +
                "title='" + title + '\'' +
                ", review=" + review +
                ", sentiment=" + sentiment +
                '}';
    }
}
