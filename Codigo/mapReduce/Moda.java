package mapReduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import mapReduce.mappers.GroupMapper;
import mapReduce.util.StatisticsJobConf;

public class Moda {
	public static class ModaReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            HashMap<Double, Integer> presence = new HashMap<Double, Integer>();
            Double value = 0.0;
            Double moda = null;
            for (DoubleWritable val : values) 
            {
            	value = val.get();
            	if (presence.containsKey(value))
            		presence.put(value, presence.get(value)+1);
            	else
            		presence.put(value, 0);
                
                if(moda==null)
                    moda = value;
                else
                    if(presence.get(value) > presence.get(moda))
                            moda = value;
            }
            result.set(moda);
            context.write(key, result);
        }
    }
	
	//TODO revisar isso
	public static void main(String[] args) throws Exception 
    {
        Job job = StatisticsJobConf.getJob(DesvioPadrao.class, "dp", GroupMapper.class, ModaReducer.class, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
