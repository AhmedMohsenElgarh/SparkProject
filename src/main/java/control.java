import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.SwingWrapper;

import javax.swing.*;

public class control {

    public JFrame draw_pieChart (Dataset<Row> data){

        PieChart pie = new PieChart(900 , 900);
        for (int i=0 ; i< 5 ; i++){
            // subList and i<in loop> control number of sectors appear in pieChart
            pie.addSeries(String.valueOf(data.collectAsList().subList(0,5).get(i).get(0)) ,
                    Integer.parseInt(String.valueOf(data.collectAsList().subList(0,10).get(i).get(1))));
        }

       return new SwingWrapper<>(pie).displayChart();
    }
}
