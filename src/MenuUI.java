import java.util.Scanner;

public class MenuUI {

    /**
     * This method displays a Text UI so the user can choose  what function they want to program to complete.
     * @param inputDirectory the directory where the input files are located
     * @param outputDirectory the directory where the output files are/will be located
     * @return an integer representing the choice made by the user.
     */

    public static int menu(String inputDirectory, String outputDirectory) {


        System.out.println("-------------------------------------------------");
        System.out.println("-------------------------------------------------");
        System.out.println("Input Directory: " + inputDirectory);
        System.out.println("Output Directory: " + outputDirectory);
        System.out.println();
        System.out.println("Choices: ");
        System.out.println("1. Get a List of Hashtags");
        System.out.println("2. Sort output");
        System.out.println("3. Get a List of English Hashtags");
        System.out.println("4. Make List of most Retweeted Tweets");
        System.out.println("5. Make List of most Retweeted Users");
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
