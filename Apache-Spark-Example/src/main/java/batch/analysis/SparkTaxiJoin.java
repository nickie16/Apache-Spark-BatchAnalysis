package batch.analysis;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

import batch.utils.Constants;
import batch.utils.Settings;
import batch.utils.SparkBS;
import batch.utils.TaxiSchema;

public class SparkTaxiJoin {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTaxiJoin.class.getName());

	public static void main(String[] args) {

		Settings settings = new Settings();
		JCommander.newBuilder()
				.addObject(settings)
				.build()
				.parse(args);

		Map<String, String> sparkConfigs = null;
		if (settings.isDisableAutoBroadcastJoin()) {
			LOG.debug("Disabling Auto broadcast");
			sparkConfigs = new HashMap<String, String>();
			sparkConfigs.put("spark.sql.autoBroadcastJoinThreshold", "-1");
		}

		SparkSession spark = SparkBS.getOrCreateInstance(settings.isLocal(), settings.getAppName(), sparkConfigs).getSession();

		final String dataFolder = settings.getDataFolder();

		// REMARK if parquet files don't exist?
		Dataset<Row> tripData = spark.read().parquet(dataFolder + settings.getTripData());

		Dataset<Row> tripVendors = spark.read().parquet(dataFolder + settings.getTripVendor());

		Dataset<Row> tripJoin = tripData.join(tripVendors.limit(100), TaxiSchema.TRIP_ID);
		tripJoin.explain();
		tripJoin.show();

		spark.close();
	}

}
