/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataindexing;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nadeesh
 */
public class IndexReducer extends Reducer<Text,Text,Text,Text>{
    
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException{
       Configuration config = HBaseConfiguration.create();
       config.set("hbase.master", "127.0.0.1:60000"); 
       
       HTable table = new HTable(config,"sex_index");
       String columnFamily = "index_data";
       String rowKey = key.toString();
       int i = 0;
       
       Put put = new Put(rowKey.getBytes());
        
        for(Text index : value)
        {
            String column = "c"+i;
            put.add(columnFamily.getBytes(),column.getBytes(),index.toString().getBytes());
            i++;
        }
        
        table.put(put);
        table.close();
    }
}
