/*
 * Classe responsável por implementar o mapper geneérico
 * Esse mapper apenas recebe uma linha e busca o formato de agrupamento e a
 * variável a ser contabilizada e passa para o reducer
 */
package mapReduce.mappers;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMQMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        Double xavg = conf.getDouble("xavg", 0.0);
        Double yavg = conf.getDouble("yavg", 0.0);

        String [] valores = value.toString().split("\\s+");
        Double x = Double.valueOf(valores[0]);
        Double y = Double.valueOf(valores[1]);
        context.write(new Text("exi"), new DoubleWritable(x));
        context.write(new Text("exiyi"), new DoubleWritable(y*x));
        context.write(new Text("exixi"), new DoubleWritable(x*x));
        context.write(new Text("nxavg"), new DoubleWritable(xavg));
        context.write(new Text("nyavg"), new DoubleWritable(yavg));
        context.write(new Text("n"), new DoubleWritable(1.0));
    }
}