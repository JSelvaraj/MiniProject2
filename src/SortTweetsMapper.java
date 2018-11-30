import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;

public class SortTweetsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    /**
     * This mapper swaps the key value pairs in the output file.
     * @param key The offset in the file.
     * @param value A line of text. This text will have come from the output of a previous MapReduce
     * @param output The file the information is output to.
     * @throws IOException If the input doesn't exist
     * @throws InterruptedException if the process is stopped midway.
     */

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
        String line = value.toString();

        Scanner scanner = new Scanner(line);

        String word = scanner.next();
        long count = scanner.nextLong();

        output.write(new LongWritable(count), new Text(word));

    }
}
