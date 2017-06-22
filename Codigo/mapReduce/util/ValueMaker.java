/*
 * Esta classe é responsável por produzir os valores utilizados no MapReduce
 */
package mapReduce.util;

import org.apache.hadoop.io.Text;

public class ValueMaker {

	public static Double valueMaker(String variavel, Text linha)
	{
		String strLinha = linha.toString().replaceAll("\\*", "");
		Double value;
		if(variavel.equals("TEMP"))
		{
			value = Double.valueOf(strLinha.substring(24, 30).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("DEWP"))
		{
			value = Double.valueOf(strLinha.substring(35, 41).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("SLP"))
		{
			value = Double.valueOf(strLinha.substring(46, 52).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("STP"))
		{
			value = Double.valueOf(strLinha.substring(57, 63).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("VISIB"))
		{
			value = Double.valueOf(strLinha.substring(68, 73).trim());
			if(value.compareTo(999.9) != 0)
				return value;
		}
		if(variavel.equals("WDSP"))
		{
			value = Double.valueOf(strLinha.substring(78, 83).trim());
			if(value.compareTo(999.9) != 0)
				return value;
		}
		if(variavel.equals("MXSPD"))
		{
			value = Double.valueOf(strLinha.substring(88, 93).trim());
			if(value.compareTo(999.9) != 0)
				return value;
		}
		if(variavel.equals("GUST"))
		{
			value = Double.valueOf(strLinha.substring(95, 100).trim());
			if(value.compareTo(999.9) != 0)
				return value;
		}
		if(variavel.equals("MAX"))
		{
			value = Double.valueOf(strLinha.substring(102, 108).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("MIN"))
		{
			value = Double.valueOf(strLinha.substring(110, 116).trim());
			if(value.compareTo(9999.9) != 0)
				return value;
		}
		if(variavel.equals("PRCP"))
		{
			value = Double.valueOf(strLinha.substring(118, 123).trim());
			if(value.compareTo(99.9) != 0)
				return value;
		}
		if(variavel.equals("SNPD"))
		{
			value = Double.valueOf(strLinha.substring(125, 130).trim());
			if(value.compareTo(999.9) != 0)
				return value;
		}
		return null;
	}

}
