package batch.utils;

import java.time.format.DateTimeFormatter;

public class Constants {

	public static final String DELIMITER = ",";
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);

	public static final String TRIP_DATA_PQ = "tripData.parquet";
	public static final String TRIP_VENDORS_PQ = "tripVendors.parquet";

	public static final double EARTH_R = 6371; // km

	// New York State Coordinates
	public static final double NY_LONG_W = -80;
	public static final double NY_LONG_E = -70;
	public static final double NY_LAT_N = 46;
	public static final double NY_LAT_S = 40;
}
