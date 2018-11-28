import org.junit.AfterClass;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.*;

public class TweetCountTest {


    private static final int MAX_MAPPERS = 4;
    private static final int MAX_THREADS = 4;
    private static final int NUMBER_OF_REPEATS = 5;
    private static final String INPUT_PATH = "/cs/scratch/js395/15-hours/*/*.json";
    private static final String OUTPUT_PATH = "output";
    private static String varyMappersDoc = "";
    private static String varyThreadsDoc = "";
    private static String varyBothDoc = "";


    @Test
    public void varyNumberOfMappers() throws IOException {
        for (int i = 1; i < MAX_MAPPERS + 1; i++) {
            long startTime = 0;
            long endTime = 0;
            varyMappersDoc += "\n" + i + " Mappers";
            for (int j = 0; j < NUMBER_OF_REPEATS; j++) {
                startTime = System.currentTimeMillis();
                new TweetCount(INPUT_PATH, OUTPUT_PATH + "/varyMappers/" + i + "/" + j, i);
                endTime = System.currentTimeMillis();
                varyMappersDoc += ", " + (endTime - startTime) ;
            }
        }
        System.out.println(varyMappersDoc);
    }

    @Test
    public void varyNumberOfThreads() throws IOException {
        for (int i = 1; i < MAX_THREADS + 1; i++) {
            long startTime = 0;
            long endTime = 0;
            varyThreadsDoc += "\n" + i + " Threads";
            for (int j = 0; j < NUMBER_OF_REPEATS; j++) {
                startTime = System.currentTimeMillis();
                new TweetCount(INPUT_PATH, OUTPUT_PATH + "/varyThreads/" + i + "/" + j, 1, i);
                endTime = System.currentTimeMillis();
                varyThreadsDoc += ", " + (endTime - startTime);
            }
        }
        System.out.println(varyThreadsDoc);
    }

    @Test
    public void varyBoth() throws IOException {
        for (int i = 2; i < MAX_MAPPERS + 1; i++) {
            long startTime = 0;
            long endTime = 0;
            varyBothDoc += "\n" + i + " Mappers";
            for (int j = 1; j < MAX_THREADS + 1; j++) {
                varyBothDoc += "\n" + j + " Threads";
                for (int k = 0; k < NUMBER_OF_REPEATS; k++) {
                    startTime = System.currentTimeMillis();
                    new TweetCount(INPUT_PATH, OUTPUT_PATH + "/varyBoth/" + i + "/" + j + "/" + k, i, j);
                    endTime = System.currentTimeMillis();
                    varyBothDoc += ", " + (endTime - startTime);
                }
            }
        }
        System.out.println(varyBothDoc);
    }

    @AfterClass
    public static void print() {
        try {
            PrintWriter writer = new PrintWriter(new FileWriter("VaryMappers.csv"));
            PrintWriter writer1 = new PrintWriter(new FileWriter("VaryThreads.csv"));
            PrintWriter writer2 = new PrintWriter(new FileWriter("varyBoth.csv"));
            writer.println(varyMappersDoc);
            writer.flush();
            writer1.println(varyThreadsDoc);
            writer1.flush();
            writer2.println(varyBothDoc);
            writer2.flush();
            writer.close();
            writer1.close();
            writer2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}