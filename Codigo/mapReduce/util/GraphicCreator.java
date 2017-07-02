//Classe que gera um Panel com o gráfico da análise feita pela GUI e com a
// funcionalidade de prever um valor através do metodo dos minimos qadrados

package mapReduce.util;

//Importacoes das bibliotecas de interface grafica e da biblioteca externa do
// JFreeChart, que fora utilizada para gerar os gráficos
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import org.jfree.chart.*;
import org.jfree.data.xy.*;
import org.jfree.chart.plot.*;
import org.jfree.chart.renderer.xy.*;

public class GraphicCreator {
    
   //Objeto do tipo JFreeChart, utilizada para armazenar o grafico gerado 
   private JFreeChart graphic;
   
   //Variaves a e b, utilizadas para calcular a reta do metodo dos minimos
   // quadrados
   public double b;
   public double a;
   
   //Atributos booleanos para identificar qual o tipo de agrupamento que foi
   // solicitado pelo usuario
   public boolean agrupadorEhY = false;
   public boolean agrupadorEhYM = false;
   public boolean agrupadorEhM = false;
   
   //Todos os valores do eixo X sao armazenados nesse vetor, para auxiliaro
   // calculo da previsao
   public String [] allValuesOfX;
   
   //Construtor da classe Graphic Creator, que garante a criacao do grafico logo
   //ao instanciar o objeto
   public GraphicCreator(String agrupador, String funcao, String results) {
    
    //Labels que serao exibidas no grafico plotado
    String chartTitle = null; //opcional
    String xAxisLabel = agrupador;
    String yAxisLabel = funcao;
 
    //Cria um dataset para o grafico baseado na saida da tela de parametrizacao
    XYDataset dataset = createDataset(results);
 
    //Cria um grafico do tipo XY
    this.graphic = ChartFactory.createXYLineChart(chartTitle,
            xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, true, true, false);
    
    //recupera o objeto que foi gerado para a variavel plot, para configura-lo
    XYPlot plot = this.graphic.getXYPlot();
    
    //Configuracoes de exibicao do grafico  
    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    renderer.setSeriesLinesVisible(0, false);
    plot.setRenderer(renderer);
    org.jfree.chart.axis.ValueAxis domain = plot.getDomainAxis();
    domain.setVisible(false);
    
    //Verifica qual agrupador foi utilizado (YYYY, YYYYMM ou MM)
    verificaAgrupador(results);         
   }
   
   //metodo que cria um Dataset
   private XYDataset createDataset(String results) {
       
        //Instancia de um novo dataset
        XYSeriesCollection dataset = new XYSeriesCollection();
        
        //Instancia de cade serie que sera exibida no grafico. No caso, teremos
        // duas representaçoes no mesmo grafico: uma distribuicao de pontos para
        // representar graficamente a consulta do usuario e a reta que melhor
        // representa essa consulta (calculada pelo metodo dos minimos 
        //quadrados)
        XYSeries series1 = new XYSeries("Distribuição de X");
        XYSeries series2 = new XYSeries("MMQ");

        //Quebra os reultados obtidos por linhas, em que cada linha recebe o 
        // valor de X e de Y
        String [] inputLines = results.split("\n"); 
        
        // Cria os auxiliares com todos os valores de X e Y
        double [] yValues = new double [inputLines.length];
        allValuesOfX = new String [inputLines.length];

       //Pega todos os valores de Y e X e armazena nos auxiliares
       for (int i = 0; i < inputLines.length; i++) {
           if (!inputLines.equals("")){
            String [] breakLine = inputLines[i].split("\t");
            yValues[i] = Double.parseDouble(breakLine[1]);
            allValuesOfX[i] = breakLine[0];
           }
       }
       
       //Adiciona a distribuicao do resultado na serie
       double xValues = 2.0;
       for (int i = 0; i<yValues.length;i++){
           series1.add(xValues,yValues[i]);
           xValues = xValues + 1.0;
       }
       
       //Chama o metodo que calcula o MMQ para gerar a reta
       calculaMMQ (xValues,yValues);
       
       //Atribui na serie a reta que melhor representa a distribuicao
       series2.add(1.0,previsaoDeValores(1.0));
       series2.add(xValues + 1.0,previsaoDeValores(xValues + 1.0));
       
       // Adiciona as series no dataset
       dataset.addSeries(series1);
       dataset.addSeries(series2);
       
       //Retorna o dataset com as informacoes que serao plotadas no grafico
       return dataset;
}
   
   //Metodo que concatena os Panels do grafico e da previsao de temperaturas
   // e retorna com um panel unico
    public JPanel getPanel() {
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));
        mainPanel.add(generatePanelOfGraphic());
        mainPanel.add(getPanelPrevisao());
        return mainPanel;
    }
    
    //Metodo que gera o Panel do grafico
    public JPanel generatePanelOfGraphic(){
        return new ChartPanel(graphic);
    }
    
    //Metodo da funcionalidade de previsao de valores, que retorna um panel
    public JPanel getPanelPrevisao(){
        
        JPanel newPanel = new JPanel(new BorderLayout());
        
        //Verifica qual tipo de agrupador foi utilizado
        String agrupador = "";
        if(agrupadorEhM) agrupador = "MM";
        else if (agrupadorEhY) agrupador = "YYYY";
        else if (agrupadorEhYM) agrupador = "YYYYMM";
        
        //Cria os objetos que o usuario ira interagir
        JLabel label = new JLabel("Previsão em...(entre com o valor no formato " + agrupador + "):");
        JTextField userInput = new JTextField(agrupador.length());
        JButton buttonLogin = new JButton("Calcular");
        JLabel label1 = new JLabel("Resultado: ");
        
        //Dispoe as posicoes dos objetos no panel
        newPanel.add(label, BorderLayout.WEST);
        newPanel.add(userInput, BorderLayout.CENTER);
        newPanel.add(buttonLogin,BorderLayout.EAST);
        newPanel.add(label1,BorderLayout.SOUTH);
        
        //Acao do botao de calcular, que pega o valor inserido pelo usuario e 
        // calcula qual sera o valor previsto
        buttonLogin.addActionListener(new ActionListener() { 
            public void actionPerformed(ActionEvent e) {
                double xValueCalculated = calculateXValueBasedOnAgregate(userInput.getText());
                double output = previsaoDeValores(xValueCalculated);
                label1.setText("Resultado: "+ output);
            } 

            
        } );
        
        //Retorna um panel com a funcionalidade
        return newPanel;
    }
    
    //Como o agrupador pode ser YYYYMM, o valor do X pode nao ser um numero 
    //"sequencialmente logico", por isso foi feito esse metodo para "converter"
    // o valor de X da previsao para manter a proporcionalidade (Exemplo:
    // 201801 deve vir imediatamente apos 201712)
    public double calculateXValueBasedOnAgregate(String xValue){
        
        //Se o agrupador for YYYY ou MM, a conversao eh simples,  pois 
        // a sequencia eh numericamente logica
        if (agrupadorEhY || agrupadorEhM){
            
            //Se o valor que o usuario quer prever for um valor futuro...
            if(Double.parseDouble(xValue) > Double.parseDouble(allValuesOfX[allValuesOfX.length-1])){
                
                //... o resultado eh a diferenca com o ultimo valor da resultado
                //acrescido do tamanho da distribuicao
                return (Double.parseDouble(xValue) - Double.parseDouble(allValuesOfX[allValuesOfX.length-1])) + (allValuesOfX.length) + 2.0;
                
              //Se o valor que o usuario quer prever for um valor passado...
            } else if (Double.parseDouble(xValue) < Double.parseDouble(allValuesOfX[0])){
                
                //...O resultado eh apenas a diferenca com o valor inicial da distribuicao
                return (Double.parseDouble(xValue) - Double.parseDouble(allValuesOfX[0])) + 2.0;
            }
            
          //Se o agrupador for YYYYMM  
        } else if (agrupadorEhYM){
            
            //Extrai os anos e os meses dos valores de X para realizar 
            // o "de-para"
            double anoDoParametro = Double.parseDouble(xValue.substring(0, 4));
            double ultimoAno = Double.parseDouble(allValuesOfX[allValuesOfX.length-1].substring(0,4));
            double primeiroAno = Double.parseDouble(allValuesOfX[0].substring(0,4));
            double mesesDoAnoDoParametro = Double.parseDouble(xValue.substring(4));
            double mesesDoUltimoAno = Double.parseDouble(allValuesOfX[allValuesOfX.length-1].substring(4));
            double mesesDoPrimeiroAno = Double.parseDouble(allValuesOfX[0].substring(4));
            
            //Se o valor que o usuario quer prever for um valor futuro...
            if(anoDoParametro > ultimoAno){
                
                //.. o resultado sera a diferenca dos anos (em meses) +
                // o modulo da diferenca dos meses + o tamanho da distribuicao
                double parte1 = (anoDoParametro - ultimoAno)*12;
                double parte2 = Math.abs(mesesDoUltimoAno-mesesDoAnoDoParametro);
                return Math.abs(parte1-parte2) + allValuesOfX.length + 2.0;
                
              //Se o valor que o usuario quer prever for um valor passado...  
            } else if (anoDoParametro < primeiroAno){
                
                //...o resultado sera a diferenca dos anos (em meses) -
                // a diferenca dos meses multiplicado por -1
                double parte1 = (anoDoParametro - primeiroAno)*12;
                double parte2 = (mesesDoAnoDoParametro-mesesDoPrimeiroAno)*-1;
                return (parte1-parte2) + 2.0;
            }
                    
        }
        return -1;
    }
    
    //Metodo que calcula os minimos quadrados
    public void calculaMMQ (double ultimoXValue, double [] yValues){
        
        //calcula media de X
        int valorDeX = (int) ultimoXValue;
        int somaDosValoresDeX = 0;
        for (int i = valorDeX; i >= 2; i--) {
            somaDosValoresDeX = somaDosValoresDeX + i;
        }
        double mediaDeX = (double) somaDosValoresDeX / (double)valorDeX;     
        
        //Calcula media de y
        double somaDosValoresDeY = 0;
        for (int j = 0; j < yValues.length; j++) {
            somaDosValoresDeY = somaDosValoresDeY + yValues[j];
        }
        double mediaDeY = somaDosValoresDeY / yValues.length;
        
        //Calcula valor de b
        
        //Numerador
        double numerador = 0;
        double inicioDeX = 2.0;
        for (int k = 0; k < yValues.length; k++) {
            numerador = numerador + (inicioDeX*(yValues[k]-mediaDeY));
            inicioDeX = inicioDeX + 1.0;
        }
        
        //Denominador
        double denominador = 0;
        inicioDeX = 2.0;
        for (int l = 0; l < yValues.length; l++) {
            denominador = denominador + (inicioDeX*(inicioDeX-mediaDeX));
            inicioDeX = inicioDeX + 1.0;
        }
        
        b = numerador/denominador;
        
        //Calcula valor de A
        a = mediaDeY - (b*mediaDeX);
        
    }
    
    //Equacao da reta para prever um valor dado um x, pois o valor de a e b 
    //ja foram calculados anteriormente
    public double previsaoDeValores(double xValue){
        return a + (b*xValue);
    }

    //Metodo que verifica qual foi o agrupador utilizado
    private void verificaAgrupador(String results) {
        String [] firstValue = results.split("\t");
        if(firstValue[0].length() == 4){
            agrupadorEhY = true;
        }else if (firstValue[0].length() == 6){
            agrupadorEhYM = true;
        } else if (firstValue[0].length() == 2) {
            agrupadorEhM = true;
        }
    }
    
}
