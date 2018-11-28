import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;

public class SortTweets {

    public static void sortTweets(String outputPath) {
        Object[] tuple = new Object[2];
        ArrayList<Object[]> list = new ArrayList<>();
        String filePath = outputPath + "/part-r-00000";


        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line = "";
            while ((line = reader.readLine()) != null) {
                Scanner scanner = new Scanner(line);
                tuple = new Object[2];
                tuple[0] = scanner.next();
                tuple[1] = scanner.nextInt();
                if (list.size() < 40) {
                    list.add(tuple);
                } else if (list.size() == 40) {
                    list.add(tuple);
                    Collections.sort(list, new keyValueComparator());
                    list.remove(list.size() - 1);
                }
            }
        } catch (IOException e) {

        }

        for (Object[] keyValue : list) {
            System.out.println(keyValue[0] + ":" + keyValue[1]);
        }

    }


}

class keyValueComparator implements Comparator<Object[]> {

    @Override
    public int compare(Object[] keyValue1, Object[] keyValue2) {
        if ((int) keyValue1[1] < (int) keyValue2[1]) {
            return 1;
        } else if ((int) keyValue1[1] == (int) keyValue2[1]) {
            return 0;
        } else {
            return -1;
        }
    }
}
