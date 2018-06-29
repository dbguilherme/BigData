package teste;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> extends Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {

    @Override
    public void reduce(KEY_IN key, Iterable<VALUE_IN> values, Context context){
        try {
            int resultNumber = 0;
            for(VALUE_IN value :  values){
               resultNumber += ((IntWritable) value).get();
//System.out.println(" resultNumber  "+ resultNumber );
            }
            IntWritable result = new IntWritable();
            result.set(resultNumber);
            context.write((KEY_OUT)key, (VALUE_OUT)result);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}