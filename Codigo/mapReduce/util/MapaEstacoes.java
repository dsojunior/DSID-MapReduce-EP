/*
 * Classe que mapeia todas as estacoes de todos os paises
 */
package mapReduce.util;

import java.io.BufferedReader;
import java.io.File;
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
    public static String REGEX;
    private static MapaEstacoes mapa;
    
    public static Map<String, List<String>> getMapaPaisEstacoes()
    {
        if(MapaEstacoes.mapa == null)
            MapaEstacoes.mapa = new MapaEstacoes();
        
        if(mapaEstacaoPais == null)
        {
            //Se o mapa ainda nao foi inicializado, inicializa-lo
            String[] split;
            
            mapaEstacaoPais = new HashMap<String, String>();
            mapaPaisEstacoes = new HashMap<String, List<String>>();
            
            try
            {
                String arquivo = MapaEstacoes.mapa.getClass().getResource("isd-history").getFile();
                FileReader arq = new FileReader(arquivo);
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
                e.printStackTrace();
            }
            
        }
        
        return mapaPaisEstacoes;    
    }
    
    public static String criarExpressaoRegular(String pais, String estacao)
    {
        Map<String, List<String>> mapaEstacoes;
        List<String> estacoes;
        String regex;
        int i;
        
        mapaEstacoes = MapaEstacoes.getMapaPaisEstacoes();
        
        if(pais.equals("TODOS"))
            return ".*";
        
        if(!estacao.equals("TODOS"))
            return ".*" + estacao + ".*";
        
        //Se nao saiu acima, montar o mapa das estacoes do pais
        regex = "";
        if(mapaEstacoes.containsKey(pais))
        {
            estacoes = mapaEstacoes.get(pais);
            String est;
            
            for(i=0; i<estacoes.size(); i++)
            {
                est = estacoes.get(i);
                regex = regex + est;
                
                if(i!=estacoes.size()-1)
                    regex = regex + "|";
            }
            
            return ".*(" + regex + ").*";
        }
        else
        {
            return ".*";
        }
    }
    
    public static String montarExpressaoRegular(String pais, String estacao)
    {
        MapaEstacoes.REGEX = MapaEstacoes.criarExpressaoRegular(pais, estacao);
        return MapaEstacoes.REGEX;
    }
    
}
