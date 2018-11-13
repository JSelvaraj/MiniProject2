
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;


public class TweetCount {

    private String inputDirectory;
    private String outputDirectory;

    public TweetCount(String input, String output) throws IOException {
        this.inputDirectory = input;
        this.outputDirectory = output;
        setup();
    }

    private void setup() throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(ScanTweetsMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(CountHashtagsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

}
