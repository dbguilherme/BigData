import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by ardilgulez on 27.11.2016.
 * Of course I would add my nickname somewhere
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    public void map(Object key, Text input, Context context){
        try {
            StringTokenizer tokenizer = new StringTokenizer(input.toString());
            while(tokenizer.hasMoreTokens()){
                Text word = new Text();
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}