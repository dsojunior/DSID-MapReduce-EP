/*
 * Classe que implementa o desvio padrao
 */
package mapReduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import mapReduce.mappers.MMQMapper;

public class MMQ {
     
    public static class MMQReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    {
        DoubleWritable result = new DoubleWritable(0.0);
        @Override
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
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3)
        {
            System.err.println("Utilizacao: <funcao> <diretorio_base_entrada> <diretorio_saida>");
            System.exit(2);
        }
        String caminho = otherArgs[1];
        String saida = otherArgs[2];
        Job job = Job.getInstance(conf, "mmq");
        job.setJarByClass(MMQ.class);
        job.setMapperClass(MMQMapper.class);
        job.setReducerClass(MMQReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(caminho));
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}