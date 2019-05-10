package batchAnalysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SparkNtuaJoin {

	private static final Logger LOG = LoggerFactory.getLogger(SparkNtuaJoin.class.getName());

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Joins").set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, String> leftArray = sc.textFile("hdfs:/nikmand/arrayOne").mapToPair(line -> {
			String[] arr = line.split("\t");
			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		JavaPairRDD<String, String> rightArray = sc.textFile("hdfs:/nikmand/arrayTwo").mapToPair(line -> {
			String[] arr = line.split("\t");
			return new Tuple2<String, String>(arr[0], arr[1]);
		});

		repartitionJoin(leftArray, rightArray);

		broadcastJoin(sc, leftArray, rightArray);

		sc.close();

	}

	public static void repartitionJoin(JavaPairRDD<String, String> leftArray, JavaPairRDD<String, String> rightArray) {
		LOG.debug("START Repartition Join");

		JavaPairRDD<String, Tuple2<String, Integer>> leftArrayTag = leftArray
				.mapToPair(tuple -> new Tuple2<String, Tuple2<String, Integer>>(tuple._1, new Tuple2<String, Integer>(tuple._2, 0)));
		JavaPairRDD<String, Tuple2<String, Integer>> rightArrayTag = rightArray
				.mapToPair(tuple -> new Tuple2<String, Tuple2<String, Integer>>(tuple._1, new Tuple2<String, Integer>(tuple._2, 1)));

		JavaPairRDD<String, Tuple2<String, Integer>> allArray = leftArrayTag.union(rightArrayTag);

		JavaRDD<String> result = allArray.groupByKey().flatMap(tuple -> {
			ArrayList<String> left = new ArrayList<String>();
			ArrayList<String> right = new ArrayList<String>();
			Iterator<Tuple2<String, Integer>> aux = tuple._2.iterator();
			while (aux.hasNext()) {
				Tuple2<String, Integer> value = aux.next();
				if (value._2 == 0) {
					left.add(value._1);
				} else {
					right.add(value._1);
				}
			}
			return left.stream().flatMap(lv -> right.stream().map(rv -> tuple._1 + ", " + lv + ", " + rv)).collect(Collectors.toList())
					.iterator();
		});

		LOG.info("Result was: ");
		result.collect().forEach(System.out::println);

		result.saveAsTextFile("hdfs:/nikmand/resultRepartition");

	}

	public static void broadcastJoin(JavaSparkContext sc, JavaPairRDD<String, String> leftArray, JavaPairRDD<String, String> rightArray) {
		LOG.debug("START Broadcast Join");

		Broadcast<List<Tuple2<String, String>>> brArray = sc.broadcast(rightArray.collect());

		MultiValuedMap<String, String> map = new ArrayListValuedHashMap<>();

		brArray.value().forEach(tuple -> map.put(tuple._1, tuple._2));

		JavaRDD<String> result = leftArray.flatMap(tuple -> {
			return map.get(tuple._1).stream().map(value -> tuple._1 + ", " + tuple._2 + ", " + value).collect(Collectors.toList())
					.iterator();
		});

		LOG.info("Result was: ");
		result.collect().forEach(System.out::println);

		result.saveAsTextFile("hdfs:/nikmand/resultBroadcast");
	}
}
