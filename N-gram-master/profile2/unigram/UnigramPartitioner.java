
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
import org.apache.hadoop.mapreduce.Partitioner;


public class UnigramPartitioner
	extends Partitioner<Text, IntWritable>{
	

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {


				String word = key.toString().substring(1);
				char partitonKey = word.toString().toLowerCase().charAt(0);
				if(Character.toString(partitonKey).matches("[a-b?]")){
				return 0;
				}
			return 1;


		}
	}
