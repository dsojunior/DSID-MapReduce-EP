/*
 * Esta classe é responsável por produzir os valores utilizados no MapReduce
 */
package mapReduce.util;

import org.apache.hadoop.io.Text;

public class ValueMaker {
    
    public static Double valueMaker(String variavel, Text linha)
    {
        String strLinha = linha.toString();
        Double tempRet;
        
        if(variavel.equals("TEMP"))
        {
            tempRet = Double.valueOf(strLinha.substring(24, 30).replace(" ", ""));
            
            if(tempRet==9999.9)
                return null;
            else
                return tempRet;
        }
        
        return null;
    }
    
}
