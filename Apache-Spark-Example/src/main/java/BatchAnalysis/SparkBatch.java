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
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring;

public class SparkBatch {
	
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("BatchAnalysis")
				  .getOrCreate();
		
		//#################################    UDFs    ##################################

		spark.udf().register("extractYear", (String s) -> {
			Date date = dateFormat.parse(s);
			return date.getYear();
		}, DataTypes.IntegerType); //example of udf
		
		spark.udf().register("isNum", (String s) -> {
			if (s==null || s.isEmpty()) return "NAN";
			char c = s.charAt(0);
			return Character.isDigit(c) ? "0" : s;
		}, DataTypes.StringType); // kati paei poly lathos	
		
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
		
		//#################################    loading data    ##################################
		// Encoders are created for Java beans
		Encoder<SearchEntry> entryEncoder = Encoders.bean(SearchEntry.class);
		
		Dataset<SearchEntry> entryDS = spark.read()  //de fainetai diafora me th dikh mas ektelesh ths askhshs h xrhsh datasets enanti df
			    .option("delimiter", "\t")
			    .option("header", "true")
			    .csv("hdfs:/user/nickiemand16/" + args[0])
			    .as(entryEncoder);
		
		//entryDS.show(false);
		
		Dataset<Row> wikiDF = spark.read()
				.option("header", "true")
				.csv("hdfs:/user/nickiemand16/" + args[1]);
		
		//wikiDF.show(false);
		
		//#################################    2.1    ##################################
		/*
		//Dataset<Row> yearDS = entryDS.withColumn("day", callUDF("extractYear",col("date"))); // example of using udf
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
		/*
		Dataset<Row> urlDS = succEntryDS.groupBy("url").agg(countDistinct(col("userid")).as("distinctVisitors")) //anti diplou group by poy xrisimopoioysame paliotera
				.filter(col("distinctVisitors").$greater(10))
				.orderBy(col("distinctVisitors").desc());
	
		urlDS.show(100,false);   // System.out.println("Count pages" + urlDS.count()); ~15 xil selides deixnoume top 100
 		*/	
		//#################################    2.4    ##################################
		/*
		Dataset<Row> keywordDS = entryDS.select("userid","keywords")
				.withColumn("keywords",explode(split(col("keywords")," ")))
				.groupBy("keywords").agg(count("*").as("Apperances"))
				.orderBy(col("Apperances").desc());
				;
		keywordDS.show(50,false);
		
		System.out.println("Distinct keywords" + keywordDS.count());
		*/
		//#################################    2.5.1    ##################################
		
		Dataset<Row> wikiWordDF = wikiDF.withColumn("title", explode(split(col("title"),"_")))
			.withColumn("firstLetter", substring(col("title"),0,1))
			.groupBy("firstLetter").agg(count("*").as("apperances"))
			.filter(col("firstLetter").isNotNull())
			.withColumn("firstLetter", callUDF("isNum",col("firstLetter")))
			.orderBy("firstLetter");
		
		wikiWordDF.show(1000,false);
		
		//System.out.println("Count words: " + wikiWordDF.count()); //38.809.498 keywords
		
	}
}