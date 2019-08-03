package com.zbdx.shuffle;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserMap extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,Context context){
		String[] line=value.toString().split("\t");
		String[] ujing=value.toString().split("\t");
		String[] uwei=value.toString().split("\t");
		StringBuffer str=new StringBuffer();
		for(int i=0;i<line.length;i++){
			if(line[i].length()<1){
				
					line[i]=null;

			}
			
			str.append(line[i]).append("\t");
			
		}
		try {
			context.write(new Text(str.toString()),new Text(" ") );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	};
	}
	

