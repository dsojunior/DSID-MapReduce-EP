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