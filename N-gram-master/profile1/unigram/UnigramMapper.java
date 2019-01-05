package unigram;

import java.io.IOException;
import java.lang.StringBuffer;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;


public class UnigramMapper extends Mapper<Object, Text, Text, NullWritable>{

    private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        
        String text = value.toString();
        if (text.isEmpty()) return;
        
        String tokenID = value.toString().split("<====>")[1];
    	StringTokenizer token = new StringTokenizer(value.toString().split("<====>")[2]);
          
        String tokenFilter = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
        while(token.hasMoreElements()) {
        String out = token.nextToken();
        // filter tokens
        	StringBuffer cleanToken = new StringBuffer();
        	for (char element : out.toCharArray()){
        		if(tokenFilter.indexOf(element) != -1)
        			cleanToken.append(element);
        	}

           	if (cleanToken.toString().isEmpty() || token.toString().isEmpty()) 
           	{
           		continue;
           	}
           	
           	
        	String ngram = cleanToken.toString();
        	ngram = ngram.replaceAll("[^A-Za-z0-9]","").toLowerCase();

        	
	//System.out.println("first test 1: " + ngram + " 2: " + tokenID);
//
			String testsplit = tokenID + "," + ngram;
       //      System.out.println("secondtest testsplit: " + testsplit);
   String[] mySplit = testsplit.toString().split(",");
	//   System.out.println("thirdtest testsplitarray: " + Arrays.toString(mySplit) + " 2: " + mySplit.length);

        
	
	if(!(mySplit.length == 2)){
	System.out.println("mapper failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");
	System.out.println("failed: -------------------------------------");

	continue;}
               String mykey = mySplit[0];  // ID
		String mykey2 = mySplit[1];  // Word

			String add = mykey + "," + mykey2;
   //System.out.println("secondtest testsplit: " + add);

            // ID(Line) with ngrams(Words) associated within the line
            //System.out.println("1: " + ngram + " 2: " + tokenID);
            
            
        	
        	// map <word, 1> and <id, 2>
		// each pair is labeled respectivly
        	
        	
            context.write(new Text(add), NullWritable.get());


        }
        return;
    }
}
