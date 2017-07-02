package mapReduce.util;


import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class GraphicCreator {
   private JFreeChart graphic;
   public double b;
   public double a;
   public boolean agrupadorEhY = false;
   public boolean agrupadorEhYM = false;
   public boolean agrupadorEhM = false;
   public String [] allValuesOfX;
   
 
   public GraphicCreator(String agrupador, String funcao, String results) {
    String chartTitle = null; //opcional, passar por parametro
    String xAxisLabel = agrupador;
    String yAxisLabel = funcao;
 
    XYDataset dataset = createDataset(results);
 
    this.graphic = ChartFactory.createXYLineChart(chartTitle,
            xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, true, true, false);
    
    XYPlot plot = this.graphic.getXYPlot();
    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    renderer.setSeriesLinesVisible(0, false);
    plot.setRenderer(renderer);
    org.jfree.chart.axis.ValueAxis domain = plot.getDomainAxis();
    domain.setVisible(false);
    
    verificaAgrupador(results);
    
            
            
   }
   
   private XYDataset createDataset(String results) {
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series1 = new XYSeries("Distribuição de X");
        XYSeries series2 = new XYSeries("MMQ");

        String [] inputLines = results.split("\n");
        double [] yValues = new double [inputLines.length];
        allValuesOfX = new String [inputLines.length];

       //Pega todos os valores de Y e X
       for (int i = 0; i < inputLines.length; i++) {
           if (!inputLines.equals("")){
            String [] breakLine = inputLines[i].split("\t");
            yValues[i] = Double.parseDouble(breakLine[1]);
            allValuesOfX[i] = breakLine[0];
           }
       }
       
       
       double xValues = 2.0;
       for (int i = 0; i<yValues.length;i++){
           series1.add(xValues,yValues[i]);
           xValues = xValues + 1.0;
       }
       
       calculaMMQ (xValues,yValues);
       
       //Faz a reta do MMQ
       series2.add(1.0,previsaoDeValores(1.0));
       series2.add(xValues + 1.0,previsaoDeValores(xValues + 1.0));
       
 
        dataset.addSeries(series1);
        dataset.addSeries(series2);
 
        return dataset;
}
   
    public JPanel getPanel() {
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));

        mainPanel.add(generatePanelOfGraphic());
        mainPanel.add(getPanelPrevisao());

        return mainPanel;
    }
    
    public JPanel generatePanelOfGraphic(){
        return new ChartPanel(graphic);
    }
    
    public JPanel getPanelPrevisao(){
        JPanel newPanel = new JPanel(new BorderLayout());
        String agrupador = "";
        if(agrupadorEhM) agrupador = "MM";
        else if (agrupadorEhY) agrupador = "YYYY";
        else if (agrupadorEhYM) agrupador = "YYYYMM";
        
        JLabel label = new JLabel("Previsão em...(entre com o valor no formato " + agrupador + "):");
        JTextField userInput = new JTextField(agrupador.length());
        JButton buttonLogin = new JButton("Calcular");
        JLabel label1 = new JLabel("Resultado: ");
        
        newPanel.add(label, BorderLayout.WEST);
        newPanel.add(userInput, BorderLayout.CENTER);
        newPanel.add(buttonLogin,BorderLayout.EAST);
        newPanel.add(label1,BorderLayout.SOUTH);
        
        buttonLogin.addActionListener(new ActionListener() { 
            public void actionPerformed(ActionEvent e) {
                double xValueCalculated = calculateXValueBasedOnAgregate(userInput.getText());
                double output = previsaoDeValores(xValueCalculated);
                label1.setText("Resultado: "+ output);
            } 

            
        } );
        
        return newPanel;
    }
    
    public double calculateXValueBasedOnAgregate(String xValue){
        if (agrupadorEhY || agrupadorEhM){
            if(Double.parseDouble(xValue) > Double.parseDouble(allValuesOfX[allValuesOfX.length-1])){
                return (Double.parseDouble(xValue) - Double.parseDouble(allValuesOfX[allValuesOfX.length-1])) + (allValuesOfX.length) + 2.0;
            } else if (Double.parseDouble(xValue) < Double.parseDouble(allValuesOfX[0])){
                return (Double.parseDouble(xValue) - Double.parseDouble(allValuesOfX[0])) + 2.0;
            }
        } else if (agrupadorEhYM){
            Double anoDoParametro = Double.parseDouble(xValue.substring(0, 4));
            Double ultimoAno = Double.parseDouble(allValuesOfX[allValuesOfX.length-1].substring(0,4));
            Double primeiroAno = Double.parseDouble(allValuesOfX[0].substring(0,4));
            double mesesDoAnoDoParametro = Double.parseDouble(xValue.substring(4));
            double mesesDoUltimoAno = Double.parseDouble(allValuesOfX[allValuesOfX.length-1].substring(4));
            double mesesDoPrimeiroAno = Double.parseDouble(allValuesOfX[0].substring(4));
            
            if(anoDoParametro > ultimoAno){
                double parte1 = (anoDoParametro - ultimoAno)*12;
                double parte2 = Math.abs(mesesDoUltimoAno-mesesDoAnoDoParametro);
                return Math.abs(parte1-parte2) + allValuesOfX.length + 2.0;
            } else if (anoDoParametro < primeiroAno){
                double parte1 = (anoDoParametro - primeiroAno)*12;
                double parte2 = (mesesDoUltimoAno-mesesDoAnoDoParametro)*-1;
                return (parte1-parte2) + 2.0;
            }
                    
        }
        return -1;
    }
    
    public void calculaMMQ (double ultimoXValue, double [] yValues){
        //calcular media de X
        int valorDeX = (int) ultimoXValue;
        int somaDosValoresDeX = 0;
        for (int i = valorDeX; i >= 2; i--) {
            somaDosValoresDeX = somaDosValoresDeX + i;
        }
        double mediaDeX = (double) somaDosValoresDeX / (double)valorDeX;     
        
        //Calcular media de y
        double somaDosValoresDeY = 0;
        for (int j = 0; j < yValues.length; j++) {
            somaDosValoresDeY = somaDosValoresDeY + yValues[j];
        }
        double mediaDeY = somaDosValoresDeY / yValues.length;
        
        //Calcular valor de b
        
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
        
        //Calcular valor de A
        a = mediaDeY - (b*mediaDeX);
        
    }
    
    public double previsaoDeValores(double xValue){
        return a + (b*xValue);
    }
    
//    public static void main(String[] args) {
//        String input = "201712\t22.5\n201801\t23.0";
//        GraphicCreator g = new GraphicCreator("Teste1", "Teste2", input);
//        JFrame frame = new JFrame("Minha janela");
//
//        JPanel mainPanel = new JPanel();
//        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));
//
//        mainPanel.add(g.getPanel());
//        mainPanel.add(g.getPanelPrevisao());
//
//        frame.add(mainPanel);
//
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        frame.pack();
//        frame.setVisible(true);
//    }

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
