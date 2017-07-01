/*
 * Esta classe é o ProgramDriver da aplicação
 */
package mapReduce.runner;

import mapReduce.Media;
import mapReduce.DesvioPadrao;
import mapReduce.Max;
import mapReduce.Min;
import mapReduce.Mediana;
import mapReduce.Moda;
import org.apache.hadoop.util.ProgramDriver;

public class Runner {
    
  public static void main(String argv[]){
    int exitCode = -1;
    
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("media", Media.class, "Programa que faz a media de uma variavel aleatoria");
      pgd.addClass("dp", DesvioPadrao.class, "Programa que calcula o desvio padrao de uma variavel aleatoria");
      pgd.addClass("max", Max.class, "Programa que encontra a maior ocorrência de uma variável aleatória");
      pgd.addClass("min", Min.class, "Programa que encontra a menor ocorrência de uma variável aleatória");
      pgd.addClass("mediana", Mediana.class, "Programa que encontra a ocorrência mediana de uma variável aleatória");
      pgd.addClass("moda", Moda.class, "Programa que encontra a ocorrência mais frequente de uma variável aleatória");
      pgd.addClass("gui", StatisticsGUI.class, "Execução através da interface gráfica");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    if(argv[0].equals("gui"))
        exitCode = 0;
    
    System.exit(exitCode);
  }
    
}
