package BatchAnalysis;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
			    .option("sep", "\t")
			    .load("")
			    .as(entryEncoder);
		
		entryDS.show();
		
	}
}