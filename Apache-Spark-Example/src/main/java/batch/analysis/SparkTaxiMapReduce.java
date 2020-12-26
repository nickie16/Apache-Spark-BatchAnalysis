package batch.analysis;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

import batch.utils.Constants;
import batch.utils.Functions;
import batch.utils.Settings;
import batch.utils.SparkBS;
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

public class SparkTaxiMapReduce {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTaxiMapReduce.class.getName());

	public static void main(String[] args) {

		Settings settings = new Settings();
		JCommander.newBuilder()
				.addObject(settings)
				.build()
				.parse(args);

		JavaSparkContext sc = SparkBS.getOrCreateInstance(settings.isLocal(), settings.getAppName()).getJavaContext();

		String dataFolder = settings.getDataFolder();

		JavaRDD<String> tripDataRDD = sc.textFile(dataFolder + settings.getTripData());

		JavaPairRDD<String, String> tripVendorsRDD = sc.textFile(dataFolder + settings.getTripVendor()).mapToPair(line -> {
			String[] arr = line.split(Constants.DELIMITER);
			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		/*
		 * NOTE: Due to the addition/removal of various columns based on the needs of each query
		 * we do not follow an object oriented approach concerning the manipulation of the data
		 */

		tripDataRDD = tripDataRDD.filter(x -> {
			String[] fields = x.split(Constants.DELIMITER);
			Long duration = Functions.timeDiff(fields[1], fields[2]);
			if (duration == null || duration <= 0) {
				return false;
			}
			double longStart = Double.parseDouble(fields[3]);
			double latiStart = Double.parseDouble(fields[4]);
			double longEnd = Double.parseDouble(fields[5]);
			double latiEnd = Double.parseDouble(fields[6]);
			if (longStart < Constants.NY_LONG_W || longStart > Constants.NY_LONG_E
					|| longEnd < Constants.NY_LONG_W || longEnd > Constants.NY_LONG_E
					|| latiStart < Constants.NY_LAT_S || latiStart > Constants.NY_LAT_N
					|| latiEnd < Constants.NY_LAT_S || latiEnd > Constants.NY_LAT_N) {
				return false;
			}
			// check for equality in start/end location
			if (longStart == longEnd && latiStart == latiEnd) {
				return false;
			}
			return true;
		});

		// 1st query

		JavaPairRDD<Integer, Long> queryOneRDD = tripDataRDD.mapToPair(x -> {
			String[] fields = x.split(Constants.DELIMITER);
			Long duration = Functions.timeDiff(fields[1], fields[2]);
			LocalDateTime timeStart = LocalDateTime.parse(fields[1], Constants.formatter); // LocalDateTime IS thread safe
			return new Tuple2<Integer, Long>(timeStart.getHour(), duration);
		});

		JavaPairRDD<Integer, Float> qurOneRDD = queryOneRDD.filter(x -> {
			return x._1 != null && x._2 != null;
		}).groupByKey().mapToPair(x -> {
			long sum = 0;
			int size = 0;
			for (Long l : x._2) {
				sum += l;
				size += 1;
			}
			return new Tuple2<Integer, Float>(x._1, (float) sum / size / 60.0F); //cast to minutes
		}).sortByKey();

		// alternately we could use reduceByKey formatting a tuple (sum, count) and then applying a map phase

		LOG.debug("Results of 1st query:");
		qurOneRDD.collect().forEach(System.out::println);

		// 2nd query

		JavaPairRDD<String, String> queryTwoRDD = tripDataRDD.mapToPair(x -> {
			String[] arr = x.split(Constants.DELIMITER, 2); // splits only on first occurrence, id will be the 1st element and all the others the 2nd
			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		JavaPairRDD<String, Tuple2<String, String>> qurTwoRDD = queryTwoRDD.join(tripVendorsRDD);
		// is there a 1-1 relationship or there are duplicates due to errors ?

		JavaPairRDD<String, Float> qTwoRDD = qurTwoRDD.mapToPair(x -> {
			Tuple2<String, String> entries = x._2; // x._2: columns of both files
			String[] arr = entries._1.split(Constants.DELIMITER);
			float cost = Float.parseFloat(arr[arr.length - 1]);
			String vendor = entries._2;
			return new Tuple2<String, Float>(vendor, cost);
		});

		JavaPairRDD<String, Float> qTwo = qTwoRDD.reduceByKey((v1, v2) -> Math.max(v1, v2)).sortByKey();

		LOG.debug("Results of 2nd query:");
		qTwo.collect().forEach(System.out::println);

		// 3rd query

		JavaRDD<ArrayList<String>> queryThreeRDD = qurTwoRDD.map(x -> {
			Tuple2<String, String> entries = x._2;
			String[] arr = entries._1.split(Constants.DELIMITER);
			String vendor = entries._2;
			ArrayList<String> fields = new ArrayList<String>(Arrays.asList(arr));
			fields.add(vendor);
			fields.add(x._1);
			return fields;
		}).filter(list -> {
			LocalDateTime timeStart = LocalDateTime.parse(list.get(0), Constants.formatter);
			return timeStart.getDayOfMonth() > 10;
		});

		JavaRDD<Tuple5<String, Double, Double, Long, String>> qurThreeRDD = queryThreeRDD.map(list -> {
			long duration = Functions.timeDiff(list.get(0), list.get(1));
			double distance = Functions.haversine(Double.parseDouble(list.get(2)), Double.parseDouble(list.get(3)),
					Double.parseDouble(list.get(4)),
					Double.parseDouble(list.get(5)));
			double speed = distance / (duration / 3600.0);
			Tuple5<String, Double, Double, Long, String> result = new Tuple5<String, Double, Double, Long, String>(list.get(8), speed,
					distance, duration, list.get(7));
			return result;
		});

		LOG.debug("Results of 3rd query:");
		qurThreeRDD.takeOrdered(5, new SortbySpeed()).forEach(System.out::println);

		sc.close();
	}
}
