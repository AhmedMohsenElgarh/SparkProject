import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.SwingWrapper;
import java.io.IOException;

import static org.apache.spark.sql.functions.*;


public class CleaningDataSet {

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org").setLevel(Level.ERROR);
        final SparkSession session = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[*]")
                .getOrCreate ();
        final DataFrameReader dataFrameReader = session.read ();


        Dataset<Row> dataset = dataFrameReader.option("header" , "true")
                .csv("/home/mohsen/IdeaProjects/JavaFinalProject/src/main/resources/Wuzzuf_Jobs.csv");

        Dataset<Row> responseWithSelectedColumns = dataset.select(
                col("title"),
                col("company"),
                col("location"),
                col("type"),
                col("level"),
                col("yearsExp"),
                col("country"),
                col("skills"));

        // Object Of DAO Class
        Dataset<DAO> DAO_Object = responseWithSelectedColumns.as(Encoders.bean(DAO.class));

        // Remove Any Duplicate From DataSet
        dataset = dataset.dropDuplicates();

        // Check For Any null Values or Missing
        System.out.println(" Check For Null " + dataset.isEmpty());

        System.out.println("=== Print 20 records of responses table ===");
        dataset.show(20);

        System.out.println("=== Summary of Data ===");
        dataset.describe().show();

        System.out.println("=== Structure of Data ===");
       dataset.printSchema();

        // Count the jobs for each company and display that in order
          Dataset<Row> jobs_for_company = DAO_Object.filter((FilterFunction<DAO>) response -> response.getTitle() != null)
                .groupBy("company")
                .count()
                .orderBy("company")
                .sort(col("count").desc()).toDF();
          // Display The Count the jobs for each compan
        jobs_for_company.show();

        // Display PieChart for Count the jobs for each company
        control con = new control();
        con.draw_pieChart(jobs_for_company);







    }

}
