/*
 * Classe que implementa a mediana
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
            	double medianaParte1 = listaValores.get(listaValores.size()/2);
            	double medianaParte2 = listaValores.get((listaValores.size()/2)+1);
            	mediana = (medianaParte1 + medianaParte2) / 2;
            }
            else{
            	mediana = listaValores.get((listaValores.size()/2)+1);
            }
            
            result.set(mediana);
            context.write(key, result);
        }
        
    }
    
    public static void main(String[] args) throws Exception 
    {
        
        /* Descricao detalhada dos argumentos
            0 - Diretorio onde se encontram os sub-diretorios de datas
            1 - Elemento que se deseja calcular a mediana
                TEMP - Temperatura media do dia     GUST - Wind Gust (N sei)
                DEWP - Sei la                       MAX  - Temperatura maxima dia
                SLP  - Pressao ao nivel do amr      MIN  - Temperatura minima dia
                STP  - Pressao atmosferica          PRCP - Precipitacao
                VISIB- Visibilidade                 SNDP - Alguma coisa de neve
                WDSP - Velocidade media do vento 	FRSHTT - Indicadores se teve neblina, chuva, neve, tempestade, trovão, tornado (talvez implementar)
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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 5) 
        {
            System.err.println("Utilizacao: mediana <diretorio_base_entrada> <elemento> <intervalo_anos> <formato_saida> <diretorio_saida>");
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
        
        Job job = Job.getInstance(conf, "mediana");
        job.setJarByClass(Mediana.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(MedianaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}