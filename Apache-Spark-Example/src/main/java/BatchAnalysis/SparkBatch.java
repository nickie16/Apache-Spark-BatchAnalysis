package BatchAnalysis;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkBatch {
	
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("BatchAnalysis")
				  .getOrCreate();

		/*
		// Create an RDD of SearchEntry objects from a text file
		JavaRDD<SearchEntry> entryRDD = spark.read()
				  .textFile("examples/src/main/resources/people.txt")
				  .javaRDD()
				  .map(line -> {
				    String[] parts = line.split("\t");
				    //Date parsedDate = dateFormat.parse(parts[2]); // we handle it in the set function
				    SearchEntry entry = new SearchEntry();
				    entry.setUserid(parts[0]);
				    entry.setKeywords(parts[1]);
				    entry.setDate(parsedDate);
				    entry.setPos(Integer.parseInt(parts[3].trim()));
				    entry.setUrl(parts[4]);
				    return entry;
				  });
		
		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		Dataset<Row> entryDF = spark.createDataFrame(entryRDD, SearchEntry.class);
		*/
		
		// Encoders are created for Java beans
		Encoder<SearchEntry> entryEncoder = Encoders.bean(SearchEntry.class);
		
		Dataset<SearchEntry> entryDS = spark.read().format("csv")
			    .option("delimiter", "\t")
			    .option("header", "true")
			    .load("hdfs:/user/nickiemand16/" + args[0])
			    .as(entryEncoder);
		
		entryDS.show(false);
		
		//#################################    2.1    ##################################
		entryDS.withColumn("day", col("date")); //use udf to get just the date or we should do it at loading

		//#################################    2.2    ##################################
		long success = entryDS.filter(col("pos").isNotNull()).count();
		long entries = entryDS.count();
		System.out.println("Total searches: " + entries);
		System.out.println("Success searches percentage: " + success * 100.0 / entries + " %");
		System.out.println("Unsuccess searches percentage: " + (entries - success) * 100.0 / entries + " %");
		 
	}
}