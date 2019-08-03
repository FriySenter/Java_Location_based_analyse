package com.zbdx.shuffle;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReduce extends Reducer<Text, Text, Text, Text>{
	protected void reduce(Text key, java.lang.Iterable<Text> value,Context context) 
			throws java.io.IOException ,InterruptedException {
		context.write(key, new Text(""));
	};
}
