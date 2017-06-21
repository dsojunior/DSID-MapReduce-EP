/*
 * Classe que implementa a media com MapReduce
 */
package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import mapReduce.util.KeyMaker;
import mapReduce.util.ValueMaker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Media {
    
    public static class MediaMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String formato = context.getConfiguration().get("formato");
            String variavel = context.getConfiguration().get("variavel");
            String chave = KeyMaker.keyMaker(formato, value);
            
            //Desconsiderar a linha de cabecalho
            if(((LongWritable) key).get() == new Long(0))
                return;
            
            //Obter o valor da variavel que se esta tirando a media
            Double valor = ValueMaker.valueMaker(variavel, value);
            
            //Se o valor retornado e nulo -> nao foi encontrada aquela variavel naquele dia
            if(valor==null)
                return;
            
            context.write(new Text(chave), new DoubleWritable(valor));
        }
    }
  
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
        
        /* Descricao detalhada dos argumentos
            0 - Diretorio onde se encontram os sub-diretorios de datas
            1 - Elemento que se deseja calcular a media
                TEMP - Temperatura media do dia     GUST - Wind Gust (N sei)
                DEWP - Sei la                       MAX  - Temperatura maxima dia
                SLP  - Pressao ao nivel do amr      MIN  - Temperatura minima dia
                STP  - Pressao atmosferica          PRCP - Precipitacao
                VISIB- Visibilidade                 SNDP - Alguma coisa de neve
                WDSP - Velocidade media do vento    
                MXSPD- Velocidade maxima do vento
            2 - Intervalo de anos a ser considerado, separado por traco ex: 1906-1909
            3 - Formato de agrupamento: 
                Y para anos 
                M para meses 
                MY para Mês-Ano
                W para dia da semana
                C para país
            4 - Diretorio de saida
        */
        
        Configuration conf = new Configuration();
        /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 5) 
        {
            System.err.println("Utilizacao: media <diretorio_base_entrada> <elemento> <intervalo_anos> <formato_saida> <diretorio_saida>");
            System.exit(2);
        }*/
        
        String variavel, formato, saida;
        //variavel = otherArgs[1];
        //formato = otherArgs[3];
        //saida = otherArgs[4];
        
        //Remover depois
        variavel = "TEMP";
        formato = "Y";
        saida = "/home/caio/gsod/saida";
        
        conf.set("variavel", variavel);
        conf.set("formato", formato);
        
        //Selecionar as pastas do intervalo de anos
        //String diretorioBase = otherArgs[0];
        //String intervaloDatas = otherArgs[2];
        
        //Remover depois
        String diretorioBase = "/home/caio/gsod";
        String intervaloDatas = "2018-2018";
        
        String[] arrDatas = intervaloDatas.split("-");
        
        int anoInicio, anoFim, anoCorrente;
        anoInicio = Integer.valueOf(arrDatas[0]);
        anoFim = Integer.valueOf(arrDatas[1]);
        
        //Colocar os diretorios percorridos na lista
        List<String> diretorios = new ArrayList<String>();
        for(anoCorrente = anoInicio; anoCorrente <= anoFim; anoCorrente++)
            diretorios.add(diretorioBase + "/" + anoCorrente);
        
        Job job = Job.getInstance(conf, "media");
        job.setJarByClass(Media.class);
        job.setMapperClass(MediaMapper.class);
        job.setCombinerClass(MediaReducer.class);
        job.setReducerClass(MediaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}