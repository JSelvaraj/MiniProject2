import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountHashtagsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * Sums the values from the map task and puts them into one sheet.
     * @param key
     * @param values
     * @param output
     * @throws IOException
     * @throws InterruptedException
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
