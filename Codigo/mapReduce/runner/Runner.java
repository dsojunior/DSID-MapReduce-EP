/*
 * Esta classe é o ProgramDriver da aplicação
 */
package mapReduce.runner;

import mapReduce.Media;
import mapReduce.DesvioPadrao;
import org.apache.hadoop.util.ProgramDriver;

public class Runner {
    
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("media", Media.class, "Programa que faz a media de uma variavel aleatoria");
      pgd.addClass("dp", DesvioPadrao.class, "Programa que calcula o desvio padrao de uma variavel aleatoria");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
    
}
