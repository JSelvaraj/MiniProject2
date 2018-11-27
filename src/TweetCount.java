
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;


public class TweetCount {


    private int MaxRunningMaps = 4;
    private int maxThreads = 1;
    private String inputDirectory;
    private String outputDirectory;


    public TweetCount(String input, String output) throws IOException {
        this.inputDirectory = input;
        this.outputDirectory = output;
        setup();
    }

    public TweetCount(String input, String output, int maxMappers) throws IOException {
        this.MaxRunningMaps = maxMappers;
        new TweetCount(input, output);
    }

    public TweetCount(String input, String output, int maxMappers, int maxThreads) throws IOException {
        this.maxThreads = maxThreads;
         new TweetCount(input,output,maxMappers);
    }


    private void setup() throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(MultithreadedMapper.class);

        MultithreadedMapper.setMapperClass(job, ScanTweetsMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, maxThreads);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(CountHashtagsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        LocalJobRunner.setLocalMaxRunningMaps(job, MaxRunningMaps);
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
