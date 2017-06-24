/*
 * Classe que implementa o desvio padrao
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
        
        /* Descricao detalhada dos argumentos
            0 - Diretorio onde se encontram os sub-diretorios de datas
            1 - Elemento que se deseja calcular a media
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
            System.err.println("Utilizacao: dp <diretorio_base_entrada> <elemento> <intervalo_anos> <formato_saida> <diretorio_saida>");
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
        
        Job job = Job.getInstance(conf, "dp");
        job.setJarByClass(DesvioPadrao.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(DpReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}