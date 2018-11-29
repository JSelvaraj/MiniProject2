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
import java.util.Scanner;

public class RetweetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

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
