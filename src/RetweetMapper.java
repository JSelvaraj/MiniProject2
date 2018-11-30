import jdk.nashorn.api.scripting.JSObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;

public class RetweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {


    /**
     *
     * This method checks that a tweet has been retweeted or is itself a retweet. Then sends the relevant tweet id and
     * retweet count to the reducer.
     *
     * @param key The Offset the value has in the file.
     * @param value The line of text representing a tweet.
     * @param output The file data is output to.
     * @throws IOException If the input doesn't exist
     * @throws InterruptedException if the process is stopped midway.
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String tweet = value.toString();

        JsonReader jsonReader = Json.createReader(new StringReader(tweet));
        JsonObject tweetObject = jsonReader.readObject();

        if (tweetObject.containsKey("retweeted_status")) {
            JsonObject innerTweet = tweetObject.getJsonObject("retweeted_status");
            if (innerTweet.containsKey("retweet_count") && innerTweet.containsKey("id_str")) {
                output.write(new Text(innerTweet.getString("id_str")), new LongWritable(innerTweet.getInt("retweet_count")));
            }
        }

        if (tweetObject.containsKey("retweet_count") && tweetObject.containsKey("id_str")) {
            JsonNumber count = tweetObject.getJsonNumber("retweet_count");
            if(count.intValue() != 0) {
                output.write(new Text(tweetObject.getString("id_str")), new LongWritable(count.intValue()));
            }
        }
    }
}
