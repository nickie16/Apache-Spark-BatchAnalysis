package batch.analysis;

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

public class SparkTaxiCreateParquet {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTaxiCreateParquet.class.getName());

	public static void main(String[] args) {

		Settings settings = new Settings();
		JCommander.newBuilder()
				.addObject(settings)
				.build()
				.parse(args);

		SparkSession spark = SparkBS.getOrCreateInstance(settings.isLocal(), settings.getAppName()).getSession();

		final String dataFolder = settings.getDataFolder();

		Dataset<Row> tripData = spark.read()
				.schema(TaxiSchema.tripDataSchema)
				.csv(dataFolder + settings.getTripData());

		Dataset<Row> tripVendors = spark.read()
				.schema(TaxiSchema.tripVendorsSchema)
				.csv(dataFolder + settings.getTripVendor());

		LOG.debug("Creating and writing parquet files to hdfs");

		// REMARK if parquet files already exist?
		tripData.write().parquet(dataFolder + Constants.TRIP_DATA_PQ);

		tripVendors.write().parquet(dataFolder + Constants.TRIP_VENDORS_PQ);

		spark.close();
	}
}
