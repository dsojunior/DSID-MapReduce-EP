/*
 * Método que configura os jobs de MapReduce
 */
package mapReduce.util;

import java.util.ArrayList;
import java.util.List;
import mapReduce.DesvioPadrao;
import mapReduce.mappers.GroupMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StatisticsJobConf {
    
    public static Job getJob(Class mainClass, String funcao, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, String[] args) throws Exception
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
            2 - Intervalo de anos a ser considerado, separado por traco ex: 19060101-19090101
            3 - Formato de agrupamento: 
                Y para anos 
                M para meses 
                MY para Mês-Ano
                W para dia da semana
                C para país
                S para estação
            4 - Diretorio de saida
            5 - Pais (Sigla como no arquivo OU TODOS - para não aplicar o filtro)
            6 - Estação (Identificador completo STN-WBAN ou TODOS - para não aplicar o filtro)        
        */
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 7) 
        {
            System.err.println("Utilizacao: <funcao> <diretorio_base_entrada> <elemento> <intervalo_anos> <formato_saida> <diretorio_saida> <pais> <estacao>");
            System.exit(2);
        }
        
        String variavel, formato, saida;
        variavel = otherArgs[1];
        formato = otherArgs[3];
        saida = otherArgs[4];
        
        //Remover depois
        //variavel = "TEMP";
        //formato = "W";
        //saida = "/home/caio/gsod/saida";
        
        conf.set("variavel", variavel);
        conf.set("formato", formato);
        
        //Selecionar as pastas do intervalo de anos
        String diretorioBase = otherArgs[0];
        String intervaloDatas = otherArgs[2];
        
        //Remover depois
        //String diretorioBase = "/home/caio/gsod";
        //String intervaloDatas = "19290101-19350101";
        conf.setInt("dataIni", Integer.valueOf(intervaloDatas.substring(0, 8)));
        conf.setInt("dataFim", Integer.valueOf(intervaloDatas.substring(9, intervaloDatas.length())));
        
        //Montar expressão regular para filtrar
        String pais = otherArgs[5];
        String estacao = otherArgs[6];
        
        //Remover depois
        //String pais = "AJ";
        //String estacao = "TODOS";
        
        conf.set("pattern", MapaEstacoes.montarExpressaoRegular(pais, estacao));
        
        int anoInicio, anoFim, anoCorrente;
        anoInicio = Integer.valueOf(intervaloDatas.substring(0,4));
        anoFim = Integer.valueOf(intervaloDatas.substring(9,13));
        
        //Colocar os diretorios percorridos na lista
        List<String> diretorios = new ArrayList<String>();
        for(anoCorrente = anoInicio; anoCorrente <= anoFim; anoCorrente++)
            diretorios.add(diretorioBase + "/" + anoCorrente);
        
        Job job = Job.getInstance(conf, funcao);
        job.setJarByClass(mainClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        for(String caminho : diretorios)
          FileInputFormat.addInputPath(job, new Path(caminho));
        
        FileInputFormat.setInputPathFilter(job, RegexIncludePathFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(saida));
        
        return job;
    }
    
}
