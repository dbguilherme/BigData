package teste;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper2<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

	
	//r<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	 //extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    @Override
    public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException{
        	System.out.println("key "+ key +"--- " + value);
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
//            while(tokenizer.hasMoreTokens()){
//                Text word = new Text();
//                word.set(tokenizer.nextToken());
                context.write((KEYOUT)key, (VALUEOUT)((IntWritable) value).get());
            //System.out.println("zxxxx" + (KEYOUT)key+"  " + (VALUEOUT) value);
            
        
    }

}