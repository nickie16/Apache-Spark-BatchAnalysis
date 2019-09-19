package batch.play;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.mean;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import batch.utils.TaxiSchema;
import scala.Tuple2;
import scala.Tuple5;

class SortbySpeed implements Serializable, Comparator<Tuple5<String, Double, Double, Long, String>> {

	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Tuple5<String, Double, Double, Long, String> a, Tuple5<String, Double, Double, Long, String> b) {
		if (a._2().equals(Double.NaN)) {
			return 1;
		}
		return b._2().compareTo(a._2()); // desc
	}
}

public class SparkNtuaTaxi {

	private static final Logger LOG = LoggerFactory.getLogger(SparkNtuaTaxi.class.getName());

	static final String TRIP_DATA = "yellow_tripdata_1m.csv";
	static final String TRIP_VENDORS = "yellow_tripvendors_1m.csv";
	static final String TRIP_DATA_PARC = "tripData.parquet";
	static final String TRIP_VENDORS_PARC = "tripVendors.parquet";

	static final String DELIMITER = ",";
	static final String DATE_FORMAT = "yyyy-mm-dd hh:mm:ss";

	static final Long timestampDiff(String start_time, String end_time) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT); // SimpleDateFormat is not thread safe so each worker uses it's own
		try {
			Date parsedDate_s = dateFormat.parse(start_time);
			Timestamp time_start = new Timestamp(parsedDate_s.getTime());
			Date parsedDate_e = dateFormat.parse(end_time);
			Timestamp time_end = new Timestamp(parsedDate_e.getTime());
			long duration = (time_end.getTime() - time_start.getTime()) / 1000;
			return duration;
		} catch (ParseException e) {
			return null;
		}
	}

	static final double haversine(Double lonStart, Double latStart, Double lonEnd, Double latEnd) {
		double earthR = 6371; // km
		double dLon = Math.toRadians(lonEnd - lonStart);
		double dLat = Math.toRadians(latEnd - latEnd);

		double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(latStart) * Math.cos(latEnd) * Math.pow(Math.sin(dLon / 2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double result = earthR * c;
		return result;
	}

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("TaxiNtua")
				.master("local[2]")
				.config("spark.sql.autoBroadcastJoinThreshold", -1) // disabling broadcast join
				.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

		StructType tripDataSchema = new StructType()
				.add(TaxiSchema.TRIP_ID, DataTypes.StringType)
				.add(TaxiSchema.TRIP_TIME_START, DataTypes.TimestampType)
				.add(TaxiSchema.TRIP_TIME_END, DataTypes.TimestampType)
				.add(TaxiSchema.TRIP_LONGITUDE_START, DataTypes.DoubleType)  // γεωγραφικό μήκος
				.add(TaxiSchema.TRIP_LATITUDE_START, DataTypes.DoubleType)   // γεωγραφικό πλάτος
				.add(TaxiSchema.TRIP_LONGITUDE_END, DataTypes.DoubleType)
				.add(TaxiSchema.TRIP_LATITUDE_END, DataTypes.DoubleType)
				.add(TaxiSchema.TRIP_COST, DataTypes.FloatType);

		StructType tripVendorsSchema = new StructType()
				.add(TaxiSchema.TRIP_ID, DataTypes.StringType)
				.add(TaxiSchema.TRIP_VENDOR, DataTypes.StringType);

		// #################################    Functions    ##################################

		spark.udf().register("haversine", (Double lonStart, Double latStart, Double lonEnd, Double latEnd) -> {
			return haversine(lonStart, latStart, lonEnd, latEnd);
		}, DataTypes.DoubleType);

		// #################################    RDD API    ##################################

		JavaRDD<String> tripDataRDD = sc.textFile("src/main/resources/yellow_tripdata_100.csv");

		JavaPairRDD<String, String> tripVendorsRDD = sc.textFile("src/main/resources/yellow_tripvendors_100.csv").mapToPair(line -> {
			String[] arr = line.split(DELIMITER);
			return new Tuple2<String, String>(arr[0], arr[1]);
		});
		// tripVendorsRDD.collect().forEach(System.out::println);

		// 1st query

		JavaPairRDD<Integer, Long> queryOneRDD = tripDataRDD.mapToPair(x -> {
			String[] fields = x.split(DELIMITER);
			Long duration = timestampDiff(fields[1], fields[2]);
			if (duration != null) {
				SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
				Date parsedDate_s = dateFormat.parse(fields[1]);
				Timestamp time_start = new Timestamp(parsedDate_s.getTime());
				return new Tuple2<Integer, Long>(time_start.getHours(), duration);
			} else {
				LOG.warn("Exception arised time_start: {}, time_end: {}", fields[1], fields[2]);
				return new Tuple2<Integer, Long>(null, null);
			}

		});

		//queryOneRDD.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Float> qurOneRDD = queryOneRDD.filter(x -> {
			return x._1 != null && x._2 != null;
		}).groupByKey().sortByKey().mapToPair(x -> {
			long sum = 0;
			int size = 0;
			for (Long l : x._2) {
				sum += l;
				size += 1;
			}
			return new Tuple2<Integer, Float>(x._1, (float) sum / size / 60.0F); //cast to minutes
		});

		LOG.debug("Retults of first query:");
		qurOneRDD.collect().forEach(System.out::println);

		// 2nd query

		JavaPairRDD<String, String> queryTwoRDD = tripDataRDD.mapToPair(x -> {
			String[] arr = x.split(DELIMITER, 2);
			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		JavaPairRDD<String, Tuple2<String, String>> qurTwoRDD = queryTwoRDD.join(tripVendorsRDD);

		//qurTwoRDD.collect().forEach(System.out::println);

		JavaPairRDD<String, Float> qTwoRDD = qurTwoRDD.mapToPair(x -> {
			Tuple2<String, String> entries = x._2;
			String[] arr = entries._1.split(DELIMITER);
			float cost = Float.parseFloat(arr[arr.length - 1]);
			String vendor = entries._2;
			return new Tuple2<String, Float>(vendor, cost);
		});

		JavaPairRDD<String, Float> qTwo = qTwoRDD.reduceByKey((v1, v2) -> Math.max(v1, v2)).sortByKey();

		// qTwo.collect().forEach(System.out::println);

		// 3rd query

		JavaRDD<ArrayList<String>> queryThreeRDD = qurTwoRDD.map(x -> {
			Tuple2<String, String> entries = x._2;
			String[] arr = entries._1.split(DELIMITER);
			String vendor = entries._2;
			ArrayList<String> fields = new ArrayList<String>(Arrays.asList(arr));
			fields.add(vendor);
			fields.add(x._1);
			return fields;
		}).filter(list -> {
			SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
			Date parsedDate_s = dateFormat.parse(list.get(0));
			Timestamp time_start = new Timestamp(parsedDate_s.getTime());
			return time_start.getDate() > 10;
		});

		//queryThreeRDD.collect().forEach(System.out::println);

		JavaRDD<Tuple5<String, Double, Double, Long, String>> qurThreeRDD = queryThreeRDD.map(list -> {
			long duration = timestampDiff(list.get(0), list.get(1));
			double distance = haversine(Double.parseDouble(list.get(2)), Double.parseDouble(list.get(3)), Double.parseDouble(list.get(4)),
					Double.parseDouble(list.get(5)));
			double speed = distance / (duration / 3600.0);
			Tuple5<String, Double, Double, Long, String> result = new Tuple5<String, Double, Double, Long, String>(list.get(8), speed,
					distance, duration, list.get(7));
			return result;
		});

		qurThreeRDD.takeOrdered(5, new SortbySpeed()).forEach(System.out::println);

		// #################################    DATAFRAME API    ##################################

		Dataset<Row> tripData = spark.read()
				.schema(tripDataSchema)
				.csv("src/main/resources/yellow_tripdata_100.csv");
		//.csv("hdfs:/user/nikmand/" + TRIP_DATA);

		Dataset<Row> tripVendors = spark.read()
				.schema(tripVendorsSchema)
				.csv("src/main/resources/yellow_tripvendors_100.csv");
		//.csv("hdfs:/user/nikmand/" + TRIP_VENDORS);

		// tripData.show();

		// tripVendors.show();

		//		tripData.write().parquet(TRIP_DATA_PARC);
		//
		//		tripVendors.write().parquet(TRIP_VENDORS_PARC);
		//
		//		Dataset<Row> tripDataPq = spark.read().parquet(TRIP_DATA_PARC);
		//
		//		Dataset<Row> tripVendorsPq = spark.read().parquet(TRIP_VENDORS_PARC);

		//#################################    Subject 1    ##################################

		Dataset<Row> tripDF = tripData.join(tripVendors, TaxiSchema.TRIP_ID);
		tripDF = tripDF.withColumn(TaxiSchema.TRIP_HOUR_START, hour(col(TaxiSchema.TRIP_TIME_START)));

		tripDF = tripDF.withColumn(TaxiSchema.TRIP_DURATION,
				(col(TaxiSchema.TRIP_TIME_END).cast(DataTypes.LongType).minus((col(TaxiSchema.TRIP_TIME_START).cast(DataTypes.LongType))))
						.divide(60));

		Dataset<Row> queryOneDF = tripDF.groupBy(TaxiSchema.TRIP_HOUR_START)
				.agg(mean(col(TaxiSchema.TRIP_DURATION)).cast(DataTypes.createDecimalType(32, 2)).as(TaxiSchema.TRIP_DURATION))
				.orderBy(TaxiSchema.TRIP_HOUR_START);

		//queryOneDF.show();

		Dataset<Row> queryTwoDF = tripDF.groupBy(TaxiSchema.TRIP_VENDOR).max(TaxiSchema.TRIP_COST);
		// queryTwoDF.show();

		// TODO first filter then apply udf
		tripDF = tripDF.withColumn(TaxiSchema.TRIP_DISTANCE,
				callUDF("haversine", col(TaxiSchema.TRIP_LONGITUDE_START), col(TaxiSchema.TRIP_LATITUDE_START),
						col(TaxiSchema.TRIP_LONGITUDE_END), col(TaxiSchema.TRIP_LATITUDE_END)))
				.withColumn(TaxiSchema.TRIP_SPEED, col(TaxiSchema.TRIP_DISTANCE).divide(col(TaxiSchema.TRIP_DURATION).divide(60.0)));

		Dataset<Row> queryThreeDF = tripDF.filter(dayofmonth(col(TaxiSchema.TRIP_TIME_START)).$greater(10))
				//.select(TaxiSchema.TRIP_ID, TaxiSchema.TRIP_TIME_START, TaxiSchema.TRIP_VENDOR, TaxiSchema.TRIP_SPEED)
				.orderBy(col(TaxiSchema.TRIP_SPEED).desc()).limit(5);

		queryThreeDF.show();

		//#################################    Subject 2    ##################################

		Dataset<Row> tripJoin = tripData.join(tripVendors.limit(100), TaxiSchema.TRIP_ID);
		//tripJoin.explain();
		//tripJoin.show();

		// περιμένουμε να γίνει broadcast join στο δεύτερο πίνακα διότι είναι μικρός
		// (το default μέγιστο μέγεθος για το οποίο έχουμε broadcast ενός πίνακα είναι 10 ΜΒ)
		// + περιγραφή broadcast join

		// με την αλλαγή χρησιμοποιεί πλέον SortMergeJoin βλέπε wiki πως γίνεται αυτό.
		// κατανεμημένα; και πάλι γίνεται shuffle αλλά αντί για hash έχουμε sort. Να αναφέρω και για τι προτιμάται από το hash.
		// αν και απαιτεί και αυτό shuffle εμφανίζει πλεονεκτήματα καθώς μπορεί να γίνει split στο δίσκο αν υπερβαίνει το μέγεθος της μνήμης
		// σε αντίθεση με το hash table που πρέπει υποχρεωτικά να χωράει.

		// αλλάζοντας τις ρυθμίσεις (ώστε να κάνει repartion join pleon?) περιμένουμε μεγαλύτερους χρόνους εκτέλεσης
		// Στο repartion join οι εγγραφές χωρίζονται σε partitions με βάση το join key και κάθε τέτοιο partition στέλνεται σε κάποιον worker
		// έτσι έχουμε shuffle πολλών εγγραφών και από τους δύο πίνακες μεταξύ κόμβων του cluster.
		// + περιγραφή repartition join.

		sc.close();

	}
}
