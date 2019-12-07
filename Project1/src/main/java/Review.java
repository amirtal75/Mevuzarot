import java.util.Objects;

public class Review {

    private String id;
    private String link;
    private String title;
    private String text;
    private int rating;
    private String author;
    private  String date;
    private int sentiment;

    public Review(String id){
        this.id = id;
        this.link = "link";
        this.title = "title";
        this.rating = -1;
        this.author = "author";
        this.date = "date";
        this.sentiment = -1;
    }

    public
    void setId(String id) {
        this.id = id;
    }

    public
    void setLink(String link) {
        this.link = link;
    }

    public
    void setTitle(String title) {
        this.title = title;
    }

    public
    void setText(String text) {
        this.text = text;
    }

    public
    void setRating(int rating) {
        this.rating = rating;
    }

    public
    void setAuthor(String author) {
        this.author = author;
    }

    public
    void setDate(String date) {
        this.date = date;
    }

    public
    void setSentiment(int sentiment) {
        this.sentiment = sentiment;
    }

    public
    int getRating() {
        return rating;
    }

    public
    int getSentiment() {
        return sentiment;
    }

    public Review(String id, String link, String title, int rating, String author, String date) {
            this.id = id;
            this.link = link;
            this.title = title;
            this.rating = rating;
            this.author = author;
            this.date = date;
            this.sentiment = -1;
        }

        public String getId() {
            return id;
        }

        public String getLink() {
            return link;
        }
        public String getTitle() {
            return title;
        }

        public String getText() {
            return text;
        }
        public int retRating() {
            return rating;
        }
        public String getAuthor() {
            return author;
        }

        public String getDate(){
            return date;
        }

    @Override
    public String toString() {
        return "Review{" +
                "id='" + id + '\'' +
                ", link='" + link + '\'' +
                ",\ntitle='" + title + '\'' +
                ", text='" + text + '\'' +
                ",\nrating=" + rating +
                ", author='" + author + '\'' +
                ",\ndate='" + date + '\'' +
                ", sentiment='" + sentiment + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Review review = (Review) o;
        return ((Review) o).getId().equals(this.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
