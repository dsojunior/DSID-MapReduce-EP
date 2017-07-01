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
       
 
        dataset.addSeries(series1);
        //dataset.addSeries(series2);
 
        return dataset;
}
   
    public JPanel getPanel() {
        return new ChartPanel(graphic);
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
