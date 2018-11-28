import java.io.IOException;

/**
 * Main class. It checks that the correct arguments have been passed in before starting the MapReduce task.
 */
public class ProjectMain {

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
            long startTime = System.currentTimeMillis();
            new TweetCount(args[0], args[1]);
            long endTime = System.currentTimeMillis();
            System.out.println(endTime - startTime);
        }
    }




}
