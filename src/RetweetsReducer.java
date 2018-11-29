import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RetweetsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     *
     * This method takes the highest value from the list of values and outputs it with the key.
     *
     *
     * @param key the unique id that represents a particular string
     * @param values the list of retweet_counts associated with that string
     * @param context the output file where all the key value pairs will be written.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long largestRetweetCount = 0;
        for (LongWritable value : values) {
            if (value.get() > largestRetweetCount) {
                largestRetweetCount = value.get();
            }
        }
        context.write(key, new LongWritable(largestRetweetCount));
    }
}
