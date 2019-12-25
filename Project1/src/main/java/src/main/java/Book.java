import java.util.ArrayList;

public class Book { // Book class

    String title;
    ArrayList<Review> reviews;

    public Book(String title, ArrayList<Review> reviews) {
        this.title = title;
        this.reviews = reviews;
    }

    public String getTitle(){
        return title;

    }

    public ArrayList<Review> getReviews(){
        return reviews;
    }

    public boolean hasReview(String reviewID){
        return reviews.indexOf(new Review(reviewID)) != -1;
    }

    public int indexOfReview(String reviewID){
        return reviews.indexOf(new Review(reviewID));
    }

    public void addSentimentAnalasys(String reviewID, String sentiment){
        int index = reviews.indexOf(new Review(reviewID));
        if (index == -1){
            System.out.println("Review with an ID: " + reviewID +" does not exist");
        }
        else{
            reviews.get(index).setSentiment(Integer.parseInt(sentiment));
            System.out.println(reviews.get(index).getSentiment());
        }
    }
}
