import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;

public class SortTweetsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
        String line = value.toString();

        Scanner scanner = new Scanner(line);

        String word = scanner.next();
        long count = scanner.nextLong();

        output.write(new LongWritable(count), new Text(word));

    }
}
