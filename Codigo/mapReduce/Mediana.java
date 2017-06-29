/*
 * Classe que implementa a mediana
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

public class Mediana {
     
    public static class MedianaReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            List<Double> listaValores = new ArrayList<Double>();
            double atual = 0;
            double mediana = 0;
                        
            for (DoubleWritable val : values) 
            {
                atual = val.get();
                listaValores.add(atual);
            }
            
            //Ordena os valores da lista
            listaValores.sort(null);
            
            if (listaValores.size() % 2 == 0){
            	double medianaParte1 = listaValores.get((listaValores.size()/2)-1);
            	double medianaParte2 = listaValores.get(listaValores.size()/2);
            	mediana = (medianaParte1 + medianaParte2) / 2;
            }
            else{
            	mediana = listaValores.get((listaValores.size()/2));
            }
            
            result.set(mediana);
            context.write(key, result);
        }
        
    }
    
    public static void main(String[] args) throws Exception 
    {

        Job job = StatisticsJobConf.getJob(DesvioPadrao.class, "mediana", GroupMapper.class, MedianaReducer.class, args);

        System.exit(job.waitForCompletion(true) ? 0 : 1);        

    }
}