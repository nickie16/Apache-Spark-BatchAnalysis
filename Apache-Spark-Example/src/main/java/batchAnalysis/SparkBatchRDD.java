package batchAnalysis;

public class SparkBatchRDD {

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
}
