package BatchAnalysis;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.dayofyear;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.weekofyear;
import static org.apache.spark.sql.functions.count;

public class SparkBatch {
	
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("BatchAnalysis")
				  .getOrCreate();

		spark.udf().register("extractYear", (String s) -> {
			Date date = dateFormat.parse(s);
			return date.getYear();
		}, DataTypes.IntegerType);
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
		
		//entryDS.show(false);
		
		//#################################    2.1    ##################################
		/*
		//Dataset<Row> yearDS = entryDS.withColumn("day", callUDF("extractYear",col("date")));
		Dataset<Row> newDS = entryDS
				.withColumn("dayofyear", dayofyear(col("date")))
				.withColumn("weekofyear", weekofyear(col("date")))
				.withColumn("month", month(col("date")))
				.persist();
		
		Dataset<Row> dayDS = newDS.groupBy("dayofyear").agg(count("*").as("SearchesPerDay")).orderBy("dayofyear");

		dayDS.show(366,false); //try with map function ?
		
		Dataset<Row> weekDS = newDS.groupBy("weekofyear").agg(count("*").as("SearchesPerWeek")).orderBy("weekofyear");
		
		weekDS.show(52,false); 
		
		Dataset<Row> monthDS = newDS.groupBy("month").agg(count("*").as("SearchesPerMonth")).orderBy("month");

		monthDS.show(12,false); // me xrhsh udf boroume na apeikonizoyme kalytera thn sthlh ths hmeromhnias
		*/
		
		//#################################    2.2    ##################################
		Dataset<SearchEntry> succEntryDS = entryDS.filter(col("pos").isNotNull());
		/*
		long success = succEntryDS.count();
		long entries = entryDS.count();
		System.out.println("Total searches: " + entries);
		System.out.println("Success searches percentage: " + success * 100.0 / entries + " %");
		System.out.println("Unsuccess searches percentage: " + (entries - success) * 100.0 / entries + " %");
		 */
		
		//#################################    2.3    ##################################
		
		//diplo group by wste na metrhsoyme episkepseis mono apo monadikous xrhstes
		//na dw countDistinct
		Dataset<Row> urlDS = succEntryDS.groupBy("url","userid").agg(count("*"))
				.groupBy("url").agg(count("*").as("distinctVisitors"))
				.filter(col("distinctVisitors").$greater(10)).orderBy("url");
		
		urlDS.orderBy("distinctVisitors").show(100,false); // System.out.println("Count pages" + urlDS.count()); ~15 xil selides deixnoume top 100
		
		
		
	
	}
}