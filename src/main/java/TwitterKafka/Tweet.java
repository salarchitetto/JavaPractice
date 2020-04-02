package TwitterKafka;

import com.google.gson.annotations.SerializedName;

public class Tweet {
    private static long ID;

    private String text;
    private String lang;
    private Users user;

    @SerializedName("retweet_count")
    private int retweetCount;

    @SerializedName("favorite_count")
    private int favoriteCount;

    public Tweet(long id, String text, String lang, Users user, int retweetCount, int favoriteCount) {
        this.ID = id;
        this.text = text;
        this.lang = lang;
        this.user = user;
        this.retweetCount = retweetCount;
        this.favoriteCount = favoriteCount;
    }

    public static long getId() {
        return ID;
    }

    public void setId(long id) {
        this.ID = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Users getUser() {
        return user;
    }

    public void setUser(Users user) {
        this.user = user;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public int getFavoriteCount() {
        return favoriteCount;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id=" + ID +
                ", text='" + text + '\'' +
                ", lang='" + lang + '\'' +
                ", user=" + user +
                ", retweetCount=" + retweetCount +
                ", favoriteCount=" + favoriteCount +
                '}';
    }
}
