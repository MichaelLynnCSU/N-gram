
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
	
    private TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(Collections.reverseOrder());
    public Map<String, Integer> occ = new HashMap<String, Integer>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
		
	
		

		
		// iterator stores word duplicates somehow
		int test = 0; 
		for(IntWritable value : values)
	 	{
		    test += value.get();   // # count of word
                   //System.out.println("1: " + mykey + " 2: " + mykey2 + " 3: " + test);
		}


   	    		   String mySplit[] = key.toString().split(",");

				if(mySplit.length == 2){
					//System.out.println(mySplit[0]);
				String go = mySplit[0] + "," + mySplit[1];
    				treeMap.put(go, test);
				//context.write(new Text(go),new IntWritable(1));
				}
				if(mySplit.length == 1){
					//System.out.println(mySplit[0]);
					String go = mySplit[0] + "," + " ";
    					treeMap.put(go, test);
					//context.write(new Text(go),new IntWritable(1));
		
				}

	}
	


	 @Override
     protected void cleanup(Context context) throws IOException, InterruptedException {
		
   /* Display content using Iterator*/


      int count = 0;
  
  
	       for(Map.Entry<String,Integer> entry : treeMap.entrySet()) {
    			 String key = entry.getKey();
       			String[] Split = key.toString().split(",");
		        String mykey = Split[0];  // ID
		        String mykey2 = Split[1];  // Word
       			Integer value = entry.getValue();
     //   System.out.println("1: " + mykey + " 2: " + mykey2 + " 3: " + value);
			if(!occ.containsKey(mykey))
				{
					occ.put(mykey,1);		
				}
			else{

				occ.put(mykey, occ.get(mykey) + 1);

				}
              }
			

		ArrayList<String> max = new ArrayList<String>();
	for(Map.Entry<String,Integer> entry : occ.entrySet()) {

			if(entry.getValue() >= 500)
				{
				   max.add(entry.getKey());
						
				}


		}


  String temp ="";
       for(Map.Entry<String,Integer> entry : treeMap.entrySet()) {
       String key = entry.getKey();
 if(key.isEmpty() || key.equals(null)){continue;}
      String[] Split = key.toString().split(",");

     String mykey = Split[0];  // ID
     String mykey2 = Split[1];  // Word
       Integer value = entry.getValue();

	


     //System.out.println(max);
	
      // System.out.println(mykey + " => " + mykey2 + " => " + value);
	
	if(max.contains(mykey))
	{
		if(temp != mykey) {count = 0;}
		count++;
		if(count <= 500)
		{
		   if(key.isEmpty() || key.equals(null))
		   {count--;}
		   else{
			String splitoutput = mykey + " " + mykey2;
		   context.write(new Text(splitoutput),new IntWritable(value));
		    temp = mykey;
			}
		}
	
	
      	}
	String splitoutput = mykey + " " + mykey2;
	   context.write(new Text(splitoutput),new IntWritable(value));
}

      
          
         
        

		 }
         
}
