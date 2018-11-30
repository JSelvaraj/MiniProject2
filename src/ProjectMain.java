import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

/**
 * Main class. It checks that the correct arguments have been passed in before starting the MapReduce task.
 */
public class ProjectMain {

    /**
     * For Testing on Palain
     */
//    private static final int MAX_MAPPERS = 12;
//    private static final int MAX_THREADS = 4;
//    private static final int NUMBER_OF_REPEATS = 10;


    /**
     * @param args The arguements to the program. Argument 1 should be a json file with tweets,
     *             argument 2 should be a filename to output to
     * @throws IOException If the tweet objects cannot be read.
     */
    public static void main(String args[]) throws IOException {

        if (args.length != 2) {
            System.out.println("Usage: java -cp \"lib/*:bin\" ProjectMain <input_path> <output_path>");
        } else {
            int choice = 0;
            String input = args[0];
            String output = args[1];
            while (choice != 9) {
                choice = MenuUI.menu(input, output);
                switch (choice) {
                    case 1:
                        TweetCount.mapReduceHashtags(input, output);
                        break;
                    case 2:
                        TweetCount.sortResults(output);
                        break;
                    case 3:
                        TweetCount.mapReduceEnglishHashtags(input, output);
                        break;
                    case 4:
                        TweetCount.mapReduceRetweets(input, output);
                        TweetCount.sortResults(output);
                        break;
                    case 5:
                        TweetCount.mapReduceMostRetweeted(input, output);
                        TweetCount.sortResults(output);
                        break;
                    case 6:
                        File outputFilePath = new File(output);
                        FileUtils.deleteDirectory(outputFilePath);
                        break;
                }
            }


            /**
             * Used for testing on Palain
             */
//            String varyBothDoc = "";
//            for (int i = 1; i < MAX_MAPPERS + 1; i++) {
//                long startTime = 0;
//                long endTime = 0;
//                varyBothDoc += "\n" + i + " Mappers";
//                for (int j = 1; j < MAX_THREADS + 1; j++) {
//                    varyBothDoc += "\n" + j + " Threads";
//                    for (int k = 0; k < NUMBER_OF_REPEATS; k++) {
//                        startTime = System.currentTimeMillis();
//                        TweetCount.MapReduceHashtags("/cs/home/js395/CS2001/2hours/*/*.json", args[1] + "/varyBoth/" + i + "/" + j + "/" + k, i, j);
//                        endTime = System.currentTimeMillis();
//                        varyBothDoc += ", " + (endTime - startTime);
//                    }
//                }
//            }
//            System.out.println(varyBothDoc);
//            PrintWriter writer = new PrintWriter(new FileWriter("Results/Results.csv"));
//            writer.println(varyBothDoc);
//            writer.flush();
//            writer.close();
//        }
        }

    }

}
