/*
 * Esta classe é responsável por produzir as chaves utilizadas no MapReduce
 */
package mapReduce.util;

import org.apache.hadoop.io.Text;

public class KeyMaker {
    
    public static String keyMaker(String agrupamento, Text linha)
    {
        String strLinha = linha.toString();
        
        if (agrupamento.equals("Y"))
        {
            return strLinha.substring(14, 18);
        }
        
        return " ";
    }
    
    public static String getHead(String agrupamento)
    {
        if (agrupamento.equals("Y"))
            return "YEAR";
        
        return "";
    }
    
}
