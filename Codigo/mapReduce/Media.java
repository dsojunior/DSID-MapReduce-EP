/*
 * Classe que implementa a media com MapReduce
 */
package mapReduce;

import java.io.IOException;

import mapReduce.mappers.GroupMapper;
import mapReduce.util.StatisticsJobConf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class Media {
  
    public static class MediaReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            double sum = 0;
            int count = 0;
            
            for (DoubleWritable val : values) 
            {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception 
    {
        
        Job job = StatisticsJobConf.getJob(Media.class, "media", GroupMapper.class, MediaReducer.class, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}