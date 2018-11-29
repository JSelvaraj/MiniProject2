import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.*;
import java.io.IOException;
import java.io.StringReader;

public class EnglishHashtagMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {

        String tweet = value.toString();

        JsonReader jsonReader = Json.createReader(new StringReader(tweet));
        JsonObject tweetObject = jsonReader.readObject();
        if (tweetObject.containsKey("lang")) {
            if (tweetObject.getString("lang").equals("en")) {
                if (tweetObject.containsKey("entities")) {
                    JsonObject entitiesObject = tweetObject.getJsonObject("entities");
                    if (entitiesObject.containsKey("hashtags")) {
                        JsonArray hashtagsArray = entitiesObject.getJsonArray("hashtags");
                        for (int i = 0; i < hashtagsArray.size(); i++) {
                            JsonObject hashtag = hashtagsArray.getJsonObject(i);
                            output.write(new Text(hashtag.getString("text")), new LongWritable(1));
                        }
                    }
                }
            }
        }
    }
}

