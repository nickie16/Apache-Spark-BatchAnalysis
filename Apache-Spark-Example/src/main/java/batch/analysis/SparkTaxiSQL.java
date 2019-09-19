package batch.analysis;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.dayofmonth;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.mean;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

import batch.utils.Constants;
import batch.utils.Functions;
import batch.utils.Settings;
import batch.utils.SparkBS;
import batch.utils.TaxiSchema;

public class SparkTaxiSQL {

	private static final Logger LOG = LoggerFactory.getLogger(SparkTaxiSQL.class.getName());

	public static void main(String[] args) {

		Settings settings = new Settings();
		JCommander.newBuilder()
				.addObject(settings)
				.build()
				.parse(args);

		SparkSession spark = SparkBS.getOrCreateInstance(settings.isLocal(), settings.getAppName()).getSession();

		final String dataFolder = settings.getDataFolder();

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

		spark.udf().register("haversine", (Double lonStart, Double latStart, Double lonEnd, Double latEnd) -> {
			return Functions.haversine(lonStart, latStart, lonEnd, latEnd);
		}, DataTypes.DoubleType);

		Dataset<Row> tripData = spark.read()
				.schema(tripDataSchema)
				.csv(dataFolder + "yellow_tripdata_1m.csv");
		//.csv(dataFolder + settings.getTripData());

		Dataset<Row> tripVendors = spark.read()
				.schema(tripVendorsSchema)
				.csv(dataFolder + "yellow_tripvendors_1m.csv");
		//.csv(dataFolder + settings.getTripVendor());

		// tripData.show();

		// tripVendors.show();

		if (settings.isCreateUseParquet()) {
			LOG.debug("Creating and reading parquet files");

			tripData.write().parquet(dataFolder + Constants.TRIP_DATA_PQ);

			tripVendors.write().parquet(dataFolder + Constants.TRIP_VENDORS_PQ);

			tripData = spark.read().parquet(dataFolder + Constants.TRIP_DATA_PQ);

			tripVendors = spark.read().parquet(dataFolder + Constants.TRIP_VENDORS_PQ);
		}

		// Data Cleaning

		// start and end destination must be different
		tripData = tripData.filter(
				(col(TaxiSchema.TRIP_LATITUDE_START).notEqual(col(TaxiSchema.TRIP_LATITUDE_END))
						.or(col(TaxiSchema.TRIP_LONGITUDE_START).notEqual(col(TaxiSchema.TRIP_LONGITUDE_END)))));

		// exclude entries with trip duration zero or less
		tripData = tripData.filter(col(TaxiSchema.TRIP_TIME_END).gt(col(TaxiSchema.TRIP_TIME_START)));

		// exclude entries with coordinates outside of NY state
		tripData = tripData.filter(col(TaxiSchema.TRIP_LATITUDE_START).between(Constants.NY_LAT_S, Constants.NY_LAT_N)
				.and(col(TaxiSchema.TRIP_LATITUDE_END).between(Constants.NY_LAT_S, Constants.NY_LAT_N))
				.and(col(TaxiSchema.TRIP_LONGITUDE_START).between(Constants.NY_LONG_W, Constants.NY_LONG_E))
				.and(col(TaxiSchema.TRIP_LONGITUDE_END).between(Constants.NY_LONG_W, Constants.NY_LONG_E)));

		tripData = tripData.withColumn(TaxiSchema.TRIP_DURATION,
				(col(TaxiSchema.TRIP_TIME_END).cast(DataTypes.LongType).minus((col(TaxiSchema.TRIP_TIME_START).cast(DataTypes.LongType))))
						.divide(60));

		Dataset<Row> tripDF = tripData.join(tripVendors, TaxiSchema.TRIP_ID);

		// 1st query

		tripDF = tripDF.withColumn(TaxiSchema.TRIP_HOUR_START, hour(col(TaxiSchema.TRIP_TIME_START)));

		Dataset<Row> queryOneDF = tripDF.groupBy(TaxiSchema.TRIP_HOUR_START)
				.agg(mean(col(TaxiSchema.TRIP_DURATION))/*.cast(DataTypes.createDecimalType(32, 2))*/.as(TaxiSchema.TRIP_DURATION))
				.orderBy(TaxiSchema.TRIP_HOUR_START);

		queryOneDF.show(50);

		// 2nd query

		Dataset<Row> queryTwoDF = tripDF.groupBy(TaxiSchema.TRIP_VENDOR).max(TaxiSchema.TRIP_COST);
		queryTwoDF.show(50);

		// 3rd query

		tripDF = tripDF.filter(dayofmonth(col(TaxiSchema.TRIP_TIME_START)).gt(10))
				.withColumn(TaxiSchema.TRIP_DISTANCE, callUDF("haversine", col(TaxiSchema.TRIP_LONGITUDE_START),
						col(TaxiSchema.TRIP_LATITUDE_START), col(TaxiSchema.TRIP_LONGITUDE_END), col(TaxiSchema.TRIP_LATITUDE_END)))
				.withColumn(TaxiSchema.TRIP_SPEED, col(TaxiSchema.TRIP_DISTANCE).divide(col(TaxiSchema.TRIP_DURATION).divide(60.0)));

		Dataset<Row> queryThreeDF = tripDF
				.select(TaxiSchema.TRIP_ID, TaxiSchema.TRIP_TIME_START, TaxiSchema.TRIP_VENDOR, TaxiSchema.TRIP_SPEED)
				.orderBy(col(TaxiSchema.TRIP_SPEED).desc()).limit(5);

		queryThreeDF.show(50);

		spark.close();

	}
}