import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Main class. It checks that the correct arguments have been passed in before starting the MapReduce task.
 */
public class ProjectMain {

    private static final int MAX_MAPPERS = 12;
    private static final int MAX_THREADS = 4;
    private static final int NUMBER_OF_REPEATS = 5;

    /**
     *
     * @param args The arguements to the program. Argument 1 should be a json file with tweets,
     *             argument 2 should be a filename to output to
     * @throws IOException If the tweet objects cannot be read.
     */
    public static void main(String args[]) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: java -cp \"lib/*:bin\"  ProjectMain <Input_Path> <Output_Path>");
        } else {
            String varyBothDoc = "";
            for (int i = 1; i < MAX_MAPPERS + 1; i++) {
                long startTime = 0;
                long endTime = 0;
                varyBothDoc += "\n" + i + " Mappers";
                for (int j = 1; j < MAX_THREADS + 1; j++) {
                    varyBothDoc += "\n" + j + " Threads";
                    for (int k = 0; k < NUMBER_OF_REPEATS; k++) {
                        startTime = System.currentTimeMillis();
                        new TweetCount(
                                args[0], args[1] + "/varyBoth/" + i + "/" + j + "/" + k, i, j);
                        endTime = System.currentTimeMillis();
                        varyBothDoc += ", " + (endTime - startTime);
                    }
                }
            }
            System.out.println(varyBothDoc);
            PrintWriter writer = new PrintWriter(new FileWriter("Results/Results.csv"));
            writer.println(varyBothDoc);
            writer.flush();
            writer.close();
        }

    }




}
