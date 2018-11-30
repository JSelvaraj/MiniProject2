import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountHashtagsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * Sums the values from the map task and puts them into one file.
     * @param key the hashtag being tracked
     * @param values I list of 1s with length equal to how many times a particular hashtag has been found in the data
     * @param output the file containing the final list of keys and values.
     * @throws IOException If the input doesn't exist
     * @throws InterruptedException if the process is stopped midway.
     */
    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
        int sum = 0;
        for (LongWritable value: values) {
            long l = value.get();
            sum += l;
        }
        output.write(key, new LongWritable(sum));
    }
}
