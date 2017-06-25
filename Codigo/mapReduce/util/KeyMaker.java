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
		if (agrupamento.equals("M"))
		{
			return strLinha.substring(18, 20);
		}
		if (agrupamento.equals("MY"))
		{
			return strLinha.substring(18, 20) + "/" + strLinha.substring(14, 18);
		}
		if (agrupamento.equals("W"))
		{
			return "SEGUNDA"; // FIXME Porque fazer EP eh uma eterna segunda
		}
		if (agrupamento.equals("C")) //TODO Codigo da estacao, para obter o pais tem que bater contra outro arquivo
		{
			return strLinha.substring(0, 6);
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
