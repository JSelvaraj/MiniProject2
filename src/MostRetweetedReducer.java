package Results;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.HashMap;

public class MostRetweetedReducer extends Reducer<Text, Text, Text, LongWritable> {

    /**
     *
     * The mapper passes this method a key and a list of values associated with that key. By using the comma to separate the
     * tweet id from the retweet_count for that tweet we can get a number for the amount of retweets for unique tweets a user has.
     *
     * While iterating through the list of values:
     * First we check if the tweet_id has been encountered before, if it hasn't we store it along with a the retweet count.
     * If it has then that means this tweet already been encountered, we check if the count we have stored or the count we are checking is higher then
     *
     * @param key the User id
     * @param values this value is a composite value made up of [tweetId,retweet_count] with a comma as a delimiter
     * @param output the final output sheet
     * @throws IOException If the input doesn't exist
     * @throws InterruptedException if the process is stopped midway.
     */

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {


        HashMap<String, Integer> tweets = new HashMap<>();

        for (Text value: values) {
            String[] split = value.toString().split(",");
            String tweetID = split[0];
            int count = Integer.parseInt(split[1]);
            if (tweets.containsKey(tweetID)) {
                if (tweets.get(tweetID) < count) {
                    tweets.put(tweetID, count);
                }
            } else {
                tweets.put(tweetID, count);
            }
        }

        int sum = 0;
        for (Integer value: tweets.values()) {
            sum += value;
        }

        output.write(key, new LongWritable(sum));
    }
}
