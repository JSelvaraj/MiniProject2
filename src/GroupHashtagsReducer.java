import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupHashtagsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    /**
     *
     * This method prints key value pairs.
     *
     * @param key A number each of the values in value have that number of instances.
     * @param values A list of strings that have appeared a number of times = to key.
     * @param output The output file being written to.
     * @throws IOException If the input doesn't exist
     * @throws InterruptedException if the process is stopped midway.
     */
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
        
        String allHashtags = "";
        for (Text value: values) {
            allHashtags += value.toString() + ", ";
        }
        output.write(key, new Text(allHashtags.substring(0,allHashtags.length()-2))); // The last character will always be trailing comma, so I removed it.
    }

}
