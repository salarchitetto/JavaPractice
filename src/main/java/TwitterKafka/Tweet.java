package TwitterKafka;

import com.google.gson.annotations.SerializedName;

public class Tweet {
    private String id;
    private String text;
    private String lang;
//    private Users user;

    @SerializedName("retweet_count")
    private int retweetCount;

    @SerializedName("favorite_count")
    private int favoriteCount;

    public Tweet(String id, String text, String lang, int retweetCount, int favoriteCount) {
        this.id = id;
        this.text = text;
        this.lang = lang;
//        this.user = user;
        this.retweetCount = retweetCount;
        this.favoriteCount = favoriteCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public int getFavoriteCount() {
        return favoriteCount;
    }

    public void setFavoriteCount(int favoriteCount) {
        this.favoriteCount = favoriteCount;
    }

    @Override
    public String toString() {
        return "{\"id\":" + "\"" + id  + "\"" + "," +
                "\"text\":" + "\"" + text + "\"" + "," +
                "\"lang\":"  + "\"" + lang + "\"" + "," +
                "\"retweetCount\":" + "\"" + retweetCount + "\"" + "," +
                "\"favoriteCount\":" + "\"" + favoriteCount + "\"" + "}";
    }
}