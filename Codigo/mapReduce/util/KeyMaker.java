/*
 * Esta classe é responsável por produzir as chaves utilizadas no MapReduce
 */
package mapReduce.util;

import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.util.Calendar;
import org.apache.hadoop.io.Text;

public class KeyMaker {

	public static String keyMaker(String agrupamento, Text linha)
	{
		String strLinha = linha.toString();
		if (agrupamento.equals("Y"))
		{
			return strLinha.substring(14, 18);
		}
		if (agrupamento.equals("M"))
		{
			return strLinha.substring(18, 20);
		}
		if (agrupamento.equals("MY"))
		{
			return strLinha.substring(14, 20);
		}
		if (agrupamento.equals("W"))
		{
                        Calendar c = Calendar.getInstance();
                        c.set(Integer.valueOf(strLinha.substring(14, 18)), Integer.valueOf(strLinha.substring(18,20)), Integer.valueOf(strLinha.substring(20, 22)));
			return new DateFormatSymbols().getWeekdays()[c.get(Calendar.DAY_OF_WEEK)];
		}
		if (agrupamento.equals("S")) 
		{
			return strLinha.substring(0, 6) + "-" + strLinha.substring(7, 12);
		}
                if (agrupamento.equals("C"))
                {
                        String chave = strLinha.substring(0, 6) + "-" + strLinha.substring(7, 12);
                        if(MapaEstacoes.mapaEstacaoPais.containsKey(chave))
                            return MapaEstacoes.mapaEstacaoPais.get(chave);
                        else
                            return "NAO_IDENTIF";
                }
		return " "; //TODO Apagar o TODO quando for certeza que vai ficar assim
	}

	public static String getHead(String agrupamento) {
		if (agrupamento.equals("Y"))
			return "YEAR";
		if (agrupamento.equals("M"))
			return "MONTH";
		if (agrupamento.equals("MY"))
			return "MONTHDAY";
		if (agrupamento.equals("W"))
			return "WEEKDAY";
		if (agrupamento.equals("C"))
			return "COUNTRY";
		return "";
	}
}
