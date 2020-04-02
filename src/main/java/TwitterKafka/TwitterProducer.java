package TwitterKafka;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Collections;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import com.google.gson.*;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.common.serialization.StringSerializer;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class TwitterProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

        //adding twitter configs
        TwitterConfigs configs = new TwitterConfigs();
        //getting twitter instance
        Twitter twitter = new TwitterFactory().getInstance();

        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



        //setting creds- visit twitter.com to get yours
        logger.info("Setting up Twitter configs");
        twitter.setOAuthConsumer(configs.TWITTER_API_KEY, configs.TWITTER_API_SECRET);
        twitter.setOAuthAccessToken(new AccessToken(configs.TWITTER_ACCESS_TOKEN, configs.TWITTER_TOKEN_SECRET));



        Authentication authentication = new OAuth1(
                configs.TWITTER_API_KEY,
                configs.TWITTER_API_SECRET,
                configs.TWITTER_ACCESS_TOKEN,
                configs.TWITTER_TOKEN_SECRET
        );

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(configs.HASHTAGS));

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        while (true) {
            try {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                logger.info(String.format("Grabbin Tweets"));
                long key = Tweet.getId();
                String msg = tweet.toString();
                ProducerRecord<Long, String> record = new ProducerRecord<>(configs.TOPIC, key, msg);
                producer.send(record);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
