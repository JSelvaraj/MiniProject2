
import Results.MostRetweetedReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class TweetCount {

    private static final int MAX_MAPPERS = 12; // During experimentation I found 12 mappers gave close to optimal runtimes (on Palain), so I gave all the jobs 12 mappers.

    /**
     * MapReduce to find hashtags in twitter data, while specifying the number of threads and mappers to use.
     * @param inputDirectory the directory where the input files are kept. Can also be an individual file
     * @param outputDirectory the directory the output will be put. This directory whould exist at the time the job is being started.
     * @param maxMappers the number of mappers the job will use.
     * @param maxThreads the number of concurrent threads on each mapper that should be used.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void MapReduceHashtags(String inputDirectory, String outputDirectory, int maxMappers, int maxThreads) throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(MultithreadedMapper.class);
        LocalJobRunner.setLocalMaxRunningMaps(job, maxMappers);

        MultithreadedMapper.setMapperClass(job, ScanTweetsMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, maxThreads);

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


    /**
     * MapReduce to find hashtags in twitter data.
     * @param inputDirectory the directory where the input files are kept. Can also be an individual file
     * @param outputDirectory the directory the output will be put. This directory whould exist at the time the job is being started.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void mapReduceHashtags(String inputDirectory, String outputDirectory) throws IOException {

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

        LocalJobRunner.setLocalMaxRunningMaps(job, MAX_MAPPERS);
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

    /**
     * MapReduce to find hashtags, while specifying the number of threads and mappers to use.
     * @param inputFolder the locations of the input file. Input file should be the output file of another mapreduce job.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void sortResults(String inputFolder) throws IOException {
        String filepath = inputFolder + "/part-r-00000";
        String outputPath = inputFolder + "/sorted";

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Sorter");

        FileInputFormat.setInputPaths(job, new Path(filepath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(SortTweetsMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        job.setReducerClass(GroupHashtagsReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);


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


    /**
     * MapReduce to find English hashtags in twitter data.
     * @param inputDirectory the directory where the input files are kept. Can also be an individual file
     * @param outputDirectory the directory the output will be put. This directory whould exist at the time the job is being started.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void mapReduceEnglishHashtags(String inputDirectory, String outputDirectory) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(EnglishHashtagMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(CountHashtagsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        LocalJobRunner.setLocalMaxRunningMaps(job, MAX_MAPPERS);

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


    /**
     * MapReduce to count retweets in a set of twitter data.
     * @param inputDirectory the directory where the input files are kept. Can also be an individual file
     * @param outputDirectory the directory the output will be put. This directory whould exist at the time the job is being started.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void mapReduceRetweets(String inputDirectory, String outputDirectory) throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(RetweetMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(RetweetsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        LocalJobRunner.setLocalMaxRunningMaps(job, MAX_MAPPERS);
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


    /**
     * MapReduce to count the number of retweets users have in a set of twitter data.
     * @param inputDirectory the directory where the input files are kept. Can also be an individual file
     * @param outputDirectory the directory the output will be put. This directory whould exist at the time the job is being started.
     * @throws IOException if the input data can't be found or the output file already exists.
     */
    public static void mapReduceMostRetweeted(String inputDirectory, String outputDirectory) throws IOException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Tweet Counter");

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        job.setMapperClass(MostRetweetedUserMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MostRetweetedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        LocalJobRunner.setLocalMaxRunningMaps(job, MAX_MAPPERS);
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
