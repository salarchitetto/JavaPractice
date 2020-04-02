package TwitterKafka;

import com.google.gson.annotations.SerializedName;

public class Users {
    private long id;
    private String name;

    @SerializedName("screen_name")
    private String screenName;
    private String location;

    @SerializedName("followers_count")
    private int followersCount;
}
