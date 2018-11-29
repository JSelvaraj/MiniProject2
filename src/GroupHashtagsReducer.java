import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupHashtagsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
        
        String allHashtags = "";
        for (Text value: values) {
            allHashtags += value.toString() + ", ";
        }
        output.write(key, new Text(allHashtags));
    }

}
