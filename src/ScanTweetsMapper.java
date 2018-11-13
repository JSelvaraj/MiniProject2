import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScanTweetsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

    }
}
