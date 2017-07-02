package mapReduce.runner;


import javax.swing.JPanel;
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
   
 
   public GraphicCreator(String agrupador, String funcao, String results) {
    String chartTitle = "Ep de DSID"; //opcional, passar por parametro
    String xAxisLabel = agrupador;
    String yAxisLabel = funcao;
 
    XYDataset dataset = createDataset(results);
 
    this.graphic = ChartFactory.createXYLineChart(null,
            xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, true, true, false);
    
    XYPlot plot = this.graphic.getXYPlot();
    XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
    renderer.setSeriesLinesVisible(0, false);
    plot.setRenderer(renderer);
    org.jfree.chart.axis.ValueAxis domain = plot.getDomainAxis();
    domain.setVisible(false);
    
            
            
   }
   
   private XYDataset createDataset(String results) {
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series1 = new XYSeries("Distribuição de X");
        XYSeries series2 = new XYSeries("MMQ");

        String [] inputLines = results.split("\n");
        double [] yValues = new double [inputLines.length];

       //Pega todos os valores de Y
       for (int i = 0; i < inputLines.length; i++) {
           if (!inputLines.equals("")){
            String [] breakLine = inputLines[i].split("\t");
            yValues[i] = Double.parseDouble(breakLine[1]);
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
        return new ChartPanel(graphic);
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
//        GraphicCreator g = new GraphicCreator("Teste1", "Teste2");
//        JFrame frame = new JFrame("Minha janela");
//        frame.add(g.getPanel());
//
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        frame.pack();
//        frame.setVisible(true);
//    }
    
}