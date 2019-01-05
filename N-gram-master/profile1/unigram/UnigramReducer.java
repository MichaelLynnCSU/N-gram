
package unigram;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashMap;
import java.util.Map;
import java.util.*;
import java.util.Map.Entry;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;

public class UnigramReducer
	extends Reducer<Text, IntWritable, Text, NullWritable>{
	
    private TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
		
		// iterator stores word duplicates somehow
	

		
		/// we can access the pairs via of text  <1,word> 
		// text access examplpe

		    		   String mySplit[] = key.toString().split(",");

				if(mySplit.length == 2){
					//System.out.println(mySplit[0]);
				//String go = mySplit[0] + " " + mySplit[1];
				String go = mySplit[1];
    				treeMap.put(go, 1);
				//context.write(new Text(go),new IntWritable(1));
				}
				if(mySplit.length == 1){
					//System.out.println(mySplit[0]);
					String go = mySplit[0];
    					treeMap.put(go, 1);
					//context.write(new Text(go),new IntWritable(1));
		
				}
         
		

	}
	


	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
		
   /* Display content using Iterator*/


      int count = 0;
       for(Map.Entry<String,Integer> entry : treeMap.entrySet()) {
       String key = entry.getKey();
       Integer value = entry.getValue();	
       //System.out.println(key + " => " + value);
	count++;
	if(count <= 500)
	{
	   if(key.isEmpty() || key.equals(null))
	   {count--;}
	   else
           context.write(new Text(key), NullWritable.get());
	}
	
      }

      
          
         
        

		 }
         
}
