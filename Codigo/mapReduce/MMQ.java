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
        DoubleWritable _a = new DoubleWritable(0.0);
        DoubleWritable _b = new DoubleWritable(0.0);
        DoubleWritable result = new DoubleWritable(0.0);
        
        static DoubleWritable _x = new DoubleWritable(0.0);
        static DoubleWritable _y = new DoubleWritable(0.0);
        static DoubleWritable _n = new DoubleWritable(0.0);
        static DoubleWritable _xx = new DoubleWritable(0.0);
        static DoubleWritable _xy = new DoubleWritable(0.0);
        
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            if(key.toString().equals("LinhaUnica"))
            {            
                Double x, y, xx, xy, n, a, b;

                Configuration conf = context.getConfiguration();
                x = _x.get();
                y = _y.get();
                xx = _xx.get();
                xy = _xy.get();
                n = _n.get();
                
                System.out.println("REDUCING: " + x + " " + y + " " + xx + " " + xy + " " + n);

                b = (xy-y)/(xx-x);
                a = (y/n) - (b*(x/n));

                _a.set(a);
                _b.set(b);

                context.write(new Text("a"), _a);
                context.write(new Text("b"), _b);

                System.out.println("PASSOU REDUCER");
                
            }
            else
            {
                Double soma = 0.0;
                String chave = key.toString();
                
                for(DoubleWritable v : values){
                    soma += v.get();
                }
                result.set(soma);

                if(chave.equals("x"))
                    _x.set(soma);
                
                if(chave.equals("y"))
                    _y.set(soma);
                
                if(chave.equals("xx"))
                    _xx.set(soma);
                
                if(chave.equals("xy"))
                    _xy.set(soma);
                
                if(chave.equals("n"))
                    _n.set(soma);

                context.write(new Text("LinhaUnica"), result);

                System.out.println("PASSOU COMBINER: " +  key.toString() + " " + soma);
            }
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
        job.setCombinerClass(MMQReducer.class);
        job.setReducerClass(MMQReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(caminho));
        FileOutputFormat.setOutputPath(job, new Path(saida));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}