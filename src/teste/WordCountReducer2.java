package teste;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer2<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> extends Reducer<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {

	 int resultNumber = 0;
    @Override
    public void reduce(KEY_IN key, Iterable<VALUE_IN> values, Context context){
        try {
           
            for(VALUE_IN value :  values){
               if(((IntWritable) value).get()> resultNumber){
            	   resultNumber= ((IntWritable) value).get();
            	   System.out.println(resultNumber +"  " );
               }
            }
            
          //  IntWritable result = new IntWritable();
           // result.set(resultNumber);
           // context.write((KEY_OUT)key, (VALUE_OUT)result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    @Override
    protected void cleanup(Context context) throws IOException,  InterruptedException {
    		Text t1 = new Text("Total Count");
    		   IntWritable result = new IntWritable();
    		context.write((KEY_OUT)t1, (VALUE_OUT) new IntWritable(resultNumber));
    }
}