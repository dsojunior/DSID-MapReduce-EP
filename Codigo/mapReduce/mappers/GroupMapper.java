/*
 * Classe responsável por implementar o mapper geneérico
 * Esse mapper apenas recebe uma linha e busca o formato de agrupamento e a
 * variável a ser contabilizada e passa para o reducer
 */
package mapReduce.mappers;

import java.io.IOException;
import mapReduce.util.KeyMaker;
import mapReduce.util.ValueMaker;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    
    Long lzero = new Long(0);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

        //Desconsiderar a linha de cabecalho
        if(((LongWritable) key).get() == lzero)
            return;

        //Obter o valor da variavel que se esta tirando a media
        Double valor = ValueMaker.valueMaker(context.getConfiguration().get("variavel"), value);

        //Se o valor retornado e nulo -> nao foi encontrada aquela variavel naquele dia
        if(valor==null)
            return;

        context.write(new Text(KeyMaker.keyMaker(context.getConfiguration().get("formato"), value)), new DoubleWritable(valor));
    }
}