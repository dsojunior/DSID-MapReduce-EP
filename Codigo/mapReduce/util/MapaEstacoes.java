/*
 * Classe que mapeia todas as estacoes de todos os paises
 */
package mapReduce.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.JOptionPane;

public class MapaEstacoes {
    
    public static Map<String, List<String>> mapaPaisEstacoes;
    public static Map<String, String> mapaEstacaoPais;
    
    public static Map<String, List<String>> getMapaPaisEstacoes()
    {
        if(mapaEstacaoPais == null)
        {
            //Se o mapa ainda nao foi inicializado, inicializa-lo
            String[] split;
            
            mapaEstacaoPais = new HashMap<String, String>();
            mapaPaisEstacoes = new HashMap<String, List<String>>();
            
            try
            {
                FileReader arq = new FileReader("isd-history.csv");
                BufferedReader lerArq = new BufferedReader(arq);
                
                String linha = lerArq.readLine();
                ArrayList<String> listaCorr;
                
                while(linha != null)
                {
                    split = linha.split(";");
                    
                    mapaEstacaoPais.put(split[1]+"-"+split[2], split[0]);
                    
                    if(mapaPaisEstacoes.containsKey(split[0]))
                    {
                        mapaPaisEstacoes.get(split[0]).add(split[1]+"-"+split[2]);
                    }
                    else
                    {
                        listaCorr = new ArrayList<String>();
                        listaCorr.add(split[1]+"-"+split[2]);
                        
                        mapaPaisEstacoes.put(split[0], listaCorr);
                    }
                    
                    linha = lerArq.readLine();
                }
            }
            catch(IOException e)
            {
                JOptionPane.showMessageDialog(null, "Não foi possível ler o arquivo de países", "Erro", JOptionPane.ERROR_MESSAGE);
            }
            
        }
        
        return mapaPaisEstacoes;    
    }
    
}
