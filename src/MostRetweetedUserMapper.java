import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;

public class MostRetweetedUserMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     *
     * @param key The offset in the file (not relevant).
     * @param value The line of text representing a tweet.
     * @param output the file being written to.
     * @throws IOException If the input doesn't exist.
     * @throws InterruptedException if the process is stopped midway.
     */
    protected void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String tweet = value.toString();

        JsonReader jsonReader = Json.createReader(new StringReader(tweet));
        JsonObject tweetObject = jsonReader.readObject();

        if (tweetObject.containsKey("retweeted_status")) {
            JsonObject innerTweet = tweetObject.getJsonObject("retweeted_status");
            if (innerTweet.containsKey("user")) {
                JsonObject user = innerTweet.getJsonObject("user");
                String userID = user.getString("id_str");
                String tweetID = innerTweet.getString("id_str");
                int count = innerTweet.getInt("retweet_count");
                String composite = tweetID + "," + count;
                output.write(new Text(userID), new Text(composite));
            }
        }

        if (tweetObject.containsKey("retweet_count") && tweetObject.containsKey("id_str")) {
            JsonNumber count = tweetObject.getJsonNumber("retweet_count");
            if (count.intValue() != 0) {
                if (tweetObject.containsKey("user")) {
                    JsonObject user = tweetObject.getJsonObject("user");
                    String userID = user.getString("id_str");
                    String tweetID = tweetObject.getString("id_str");
                    output.write(new Text(userID), new Text(tweetID + "," + count));
                }
            }
        }


    }
}
