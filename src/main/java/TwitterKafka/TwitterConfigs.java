package TwitterKafka;

import com.google.common.collect.Lists;

import java.util.List;

public class TwitterConfigs {

    //kafka configs
    public final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public final String GROUP_ID = "my-sixth-application";
    public final String TOPIC = "twitter-topic";
    public static final long SLEEP = 1000;


    //twitter configs
    public final String TWITTER_API_KEY = "";
    public final String TWITTER_API_SECRET = "";
    public final String TWITTER_ACCESS_TOKEN = "";
    public final String TWITTER_TOKEN_SECRET = "";

    //    public String[] HASHTAGS = new String[]{"Juventus", "Real Madrid", "Manchester United", "PSG", "Liverpool"};
    public final String HASHTAGS = "Juventus";
    public List<String> TERMS_HASHTAGS = Lists.newArrayList("Juventus", "Real Madrid", "Manchester United", "PSG", "Liverpool" );

}