package batch.utils;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBS {

	private static final Logger LOG = LoggerFactory.getLogger(SparkBS.class);

	private static SparkBS instance = null;

	private SparkConf sparkConf = null;
	private SparkSession ss = null;
	private SparkContext sc = null;
	private JavaSparkContext jsc = null;

	private SparkBS(Boolean isLocal, String appName, Map<String, String> sparkConfigs) {
		LOG.debug("constructor: isLocal={}, appName={}, sparkConfigs={}", isLocal, appName, sparkConfigs);

		sparkConf = new SparkConf();

		if (appName != null) {
			sparkConf.setAppName(appName);
		}

		if (isLocal) {
			sparkConf.setMaster("local[1]");
		}
		/* else {
			sparkConf.setMaster("yarn");
		} */

		if (sparkConfigs != null) {
			sparkConfigs.forEach((key, value) -> {
				sparkConf.set(key, value);
			});
		}

		Builder builder = SparkSession.builder()
				.config(sparkConf);

		ss = builder.getOrCreate();
		sc = ss.sparkContext();
		jsc = new JavaSparkContext(sc);

	}

	public static SparkBS getOrCreateInstance(Boolean isLocal, String appName, Map<String, String> sparkConfigs) {

		LOG.debug("START getOrCreateInstance: isLocal={}, appName={}, sparkConfigs={}", isLocal, appName, sparkConfigs);

		if (instance == null) {
			LOG.info("Creating new SparkBS instance");
			instance = new SparkBS(isLocal, appName, sparkConfigs);
		}

		return instance;
	}

	public static SparkBS getOrCreateInstance(Boolean isLocal, String appName) {
		return getOrCreateInstance(isLocal, appName, null);
	}

	public SparkSession getSession() {
		return ss;
	}

	public SparkContext getContext() {
		return sc;
	}

	public JavaSparkContext getJavaContext() {
		return jsc;
	}
}
