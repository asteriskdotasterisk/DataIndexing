/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataindexing;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author nadeesh
 */
public class DataIndexing {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path path = new Path("output");
        if(fs.exists(path))
            fs.delete(path, true);
        Job job = new Job(config);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/data/migration.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.setJarByClass(IndexMapper.class);
        job.waitForCompletion(true);
    
    }
}
