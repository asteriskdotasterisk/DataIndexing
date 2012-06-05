package dataindexing;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author nadeesh
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{
    Text word = new Text();
    Text k = new Text();
    Text v = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException 
    { 
       String line = value.toString();
       String[] words = line.split(",");
       String batch_id = words[0].substring(1,words[0].length()-1);
       String hhld = words[1].substring(1,words[1].length()-1);
       String idcode = words[2].substring(1,words[2].length()-1);
      // int sex = Integer.parseInt(words[3].substring(1,words[3].length()-1));
       String sex = words[3].substring(1,words[3].length()-1);
       
       String row_index = batch_id+":"+hhld+":"+idcode;
       
       k.set(sex);
       v.set(row_index);
       
       context.write(k, v);
    } 
    
}
