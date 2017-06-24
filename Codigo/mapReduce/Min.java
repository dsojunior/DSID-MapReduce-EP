/*
 * Classe que implementa a min com MapReduce
 */
package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import mapReduce.mappers.GroupMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class Min {
  
    public static class MinReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            Double value = Double.MAX_VALUE;
            for (DoubleWritable val : values) 
            {
                value = value.compareTo(val.get()) < 0 ? value : val.get();
            }
            context.write(key, result);
        }
    }
//TODO revisar isso
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 5) 
        {
            System.err.println("Utilizacao: min <diretorio_base_entrada> <elemento> <intervalo_anos> <formato_saida> <diretorio_saida>");
            System.exit(2);
        }
        
        String variavel, formato, saida;
        variavel = otherArgs[1];
        formato = otherArgs[3];
        saida = otherArgs[4];
        
        //Remover depois
        //variavel = "TEMP";
        //formato = "Y";
        //saida = "/home/caio/gsod/saida";
        
        conf.set("variavel", variavel);
        conf.set("formato", formato);
        
        //Selecionar as pastas do intervalo de anos
        String diretorioBase = otherArgs[0];
        String intervaloDatas = otherArgs[2];
        
        //Remover depois
        //String diretorioBase = "/home/caio/gsod";
        //String intervaloDatas = "2018-2018";
        
        String[] arrDatas = intervaloDatas.split("-");
        
        int anoInicio, anoFim, anoCorrente;
        anoInicio = Integer.valueOf(arrDatas[0]);
        anoFim = Integer.valueOf(arrDatas[1]);
        
        //Colocar os diretorios percorridos na lista
        List<String> diretorios = new ArrayList<String>();
        for(anoCorrente = anoInicio; anoCorrente <= anoFim; anoCorrente++)
            diretorios.add(diretorioBase + "/" + anoCorrente);
        
        Job job = Job.getInstance(conf, "min");
        job.setJarByClass(Min.class);
        job.setMapperClass(GroupMapper.class);
        job.setCombinerClass(MinReducer.class);
        job.setReducerClass(MinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}