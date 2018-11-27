import org.junit.AfterClass;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.*;

public class TweetCountTest {

    private static final int NUMBER_OF_REPEATS = 3;
    private static final String INPUT_PATH = "/cs/scratch/js395/27/00/*.json";
    private static final String OUTPUT_PATH = "output";
    private static String varyMappersDoc = "";
    private static String varyThreadsDoc = "";
    private static String varyBothDoc = "";



    @Test
    public void varyNumberOfMappers () throws IOException {
        for (int i = 1; i < 5 ; i++) {
            long startTime = 0;
            long endTime = 0;
            varyMappersDoc += "\nMappers "+ i;
            for (int j = 0; j < NUMBER_OF_REPEATS ; j++) {
                startTime = System.currentTimeMillis();
                new TweetCount(INPUT_PATH,OUTPUT_PATH+"/varyMappers/" + i + "/" + j, i);
                endTime = System.currentTimeMillis();
                varyMappersDoc += ", " + (endTime - startTime);
            }
        }
        System.out.println(varyMappersDoc);
    }

    @Test
    public void varyNumberOfThreads () throws IOException {
        for (int i = 0; i < 4 ; i++) {
            long startTime = 0;
            long endTime = 0;
            varyThreadsDoc += "\nThreads "+ i;
            for (int j = 0; j < NUMBER_OF_REPEATS ; j++) {
                startTime = System.currentTimeMillis();
                new TweetCount(INPUT_PATH,OUTPUT_PATH+"/varyThreads/" + i + "/" + j, i);
                endTime = System.currentTimeMillis();
                varyThreadsDoc += ", " + (endTime - startTime);
            }
        }
        System.out.println(varyThreadsDoc);
    }

    @AfterClass
    public static void print() {
        try {
            PrintWriter writer = new PrintWriter(new FileWriter("VaryMappers.csv"));
            PrintWriter writer1 = new PrintWriter(new FileWriter("VaryThreads.csv"));
            writer.println(varyMappersDoc);
            writer.flush();
            writer1.println(varyThreadsDoc);
            writer1.flush();
            writer.close();
            writer1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}