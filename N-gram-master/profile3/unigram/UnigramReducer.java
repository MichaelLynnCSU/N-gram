
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

public class UnigramReducer
	extends Reducer<Text, IntWritable, Text, IntWritable>{
	
    private Map<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
		
	
		
	
		int test = 0; 
		for(IntWritable value : values)
	 	{
		    test += value.get();   // # count of word
                   //System.out.println("1: " + mykey + " 2: " + mykey2 + " 3: " + test);
		}


       
           
   	    		   String mySplit[] = key.toString().split(",");

				if(mySplit.length == 2){
					//System.out.println(mySplit[0]);
				String go = mySplit[0] + " " + mySplit[1];
    				map.put(go, test);
				//context.write(new Text(go),new IntWritable(1));
				}
				if(mySplit.length == 1){
					//System.out.println(mySplit[0]);
					String go = mySplit[0];
    					map.put(go, test);
					//context.write(new Text(go),new IntWritable(1));
		
				}
         
		

	}
	


	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
		 int count = 0;


	// borrowed this hashmap sort by value from
	// http://www.java2novice.com/java-interview-programs/sort-a-map-by-value/
   /* Display content using Iterator*/
	Set<Entry<String, Integer>> set = map.entrySet();
        List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );


        for(Map.Entry<String, Integer> entry:list){

        	//System.out.println(entry.getKey()+" ==== "+entry.getValue());
        String key = entry.getKey();
       Integer value = entry.getValue();	
      // System.out.println(key + " => " + value);
	count++;
	if(count <= 500)
	{
	   if(key.isEmpty() || key.equals(null))
	   {count--;}
	   else
           context.write(new Text(key),new IntWritable(value));
	}
        }
	
}

 
         
}
