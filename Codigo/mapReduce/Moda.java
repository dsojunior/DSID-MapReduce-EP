package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapReduce.Min.MinReducer;
import mapReduce.mappers.GroupMapper;

public class Moda {
	public static class ModaReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            HashMap<Double, Integer> presence = new HashMap<Double, Integer>();
            Double value = 0.0;
            Double moda = 0.0;
            for (DoubleWritable val : values) 
            {
            	value = val.get();
            	if (presence.containsKey(value))
            		presence.put(value, presence.get(value)+1);
            	else
            		presence.put(value, 0);
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
        
        Job job = Job.getInstance(conf, "moda");
        job.setJarByClass(Moda.class);
        job.setMapperClass(GroupMapper.class);
        job.setCombinerClass(ModaReducer.class);
        job.setReducerClass(ModaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
