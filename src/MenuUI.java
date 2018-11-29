import java.util.Scanner;

public class MenuUI {


    public static int menu(String inputDirectory, String outputDirectory) {

        System.out.println("-------------------------------------------------");
        System.out.println("-------------------------------------------------");
        System.out.println("Input Directory: " + inputDirectory);
        System.out.println("Output Directory: " + outputDirectory);
        System.out.println();
        System.out.println("Choices: ");
        System.out.println("1. MapReduce Hashtags");
        System.out.println("2. Sort output");
        System.out.println("3. Get English Hashtags only");
        System.out.println("4. MapReduce retweets");
        System.out.println("5. Most Retweeted");
        System.out.println("6. Delete Output Directory (for running more than one MapReduce)");
        System.out.println("9. Exit");
        System.out.println();
        System.out.print("Choice: ");
        Scanner kb = new Scanner(System.in);
        String choice = kb.nextLine();
        System.out.println("------------------------------------------------");
        System.out.println("------------------------------------------------");
        return Integer.parseInt(choice);
    }


}
