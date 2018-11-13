public class ProjectMain {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.println("Usage: java ProjectMain <Input Path> <Output Path>");
        } else {
            TweetCount tweetCounter = new TweetCount(args[0], args[1]);
        }





    }




}
