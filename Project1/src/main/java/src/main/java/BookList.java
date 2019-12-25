import java.util.ArrayList;

public class BookList {

    ArrayList<Book> list;

    public BookList() {
        this.list = new ArrayList<>();
    }

    public ArrayList<Book> getList() {
        return list;
    }
    public void add(Book book){
        list.add(book);
    }

    public int indexofReview(String reviewID){
        for (Book b:
                list) {
            int index = b.indexOfReview(reviewID);
            if (index != -1){
                return index;
            }
        }
        return -1;
    }

    public Boolean hasReview(String reviewID){
        return indexofReview(reviewID) != -1;
    }

    public void addSentiment(String reviewID, String setiment){
        for (Book b:
                list) {
            int index = b.indexOfReview(reviewID);
            if (index != -1){
                b.addSentimentAnalasys(reviewID, setiment);
            }
        }
    }

    public Review getReview(String reviewID){
        for (Book b:
                list) {
            int index = b.indexOfReview(reviewID);
            if (index != -1){
               return b.getReviews().get(index);
            }
        }
        return null;
    }
}



