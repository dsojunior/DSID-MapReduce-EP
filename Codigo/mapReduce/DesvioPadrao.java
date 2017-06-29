/*
 * Classe que implementa o desvio padrao
 */
package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapReduce.mappers.GroupMapper;
import mapReduce.util.StatisticsJobConf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class DesvioPadrao {
     
    public static class DpReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            List<Double> listaValores = new ArrayList<Double>();
            double sum = 0;
            double media = 0;
            double desvAcum = 0;
            double corrente = 0;
            int count = 0;
            
            for (DoubleWritable val : values) 
            {
                corrente = val.get();
                sum += corrente;
                
                //E necessario usar uma lista porque nao tem como percorrer um iterator duas vezes
                listaValores.add(corrente);
                count++;
            }
            media = (sum/count);
            
            for (Double valorCorrente : listaValores)
            {
                //Acumular os desvios quadraticos
                desvAcum += Math.pow((valorCorrente-media),2);
            }
            
            if(count==1)
            {
                //Se so ha um elemento, o desvio padrao e zero
                result.set(0);
            }
            else
            {
                //Calculo do desvio
                result.set(Math.sqrt(desvAcum/(count-1)));
            }
            
            context.write(key, result);
        }
        
    }
    
    public static void main(String[] args) throws Exception 
    {
        Job job = StatisticsJobConf.getJob(DesvioPadrao.class, "dp", GroupMapper.class, DpReducer.class, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}