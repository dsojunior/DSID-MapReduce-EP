/*
 * Classe que implementa o desvio padrao
 */
package mapReduce;

import java.io.IOException;
import mapReduce.util.StatisticsJobConf;
import mapReduce.mappers.MMQMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class MMQ {
     
    public static class MMQReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        DoubleWritable result = new DoubleWritable(0.0);
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            Double soma = 0.0;
            for(DoubleWritable v : values){
                soma += v.get();
            }
            result.set(soma);
            context.write(key, result);
        }   
    }
    
    public static void main(String[] args) throws Exception 
    {
        //TODO A ENTRADA DESSE AQUI É OUTRA, COMO CONVERSAMOS, È UM ARQUIVO COM X Y por linha
        Job job = StatisticsJobConf.getJob(MMQ.class, "mmq", MMQMapper.class, MMQReducer.class, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}