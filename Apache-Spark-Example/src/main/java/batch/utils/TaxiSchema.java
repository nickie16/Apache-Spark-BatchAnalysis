package batch.utils;

public class TaxiSchema {

	// tripData fields

	public static final String TRIP_ID = "id";
	public static final String TRIP_TIME_START = "start_time";
	public static final String TRIP_TIME_END = "end_time";
	public static final String TRIP_LONGITUDE_START = "start_longitude";
	public static final String TRIP_LATITUDE_START = "start_latitude";
	public static final String TRIP_LONGITUDE_END = "end_longitude";
	public static final String TRIP_LATITUDE_END = "end_latitude";
	public static final String TRIP_COST = "cost";

	// tripVendors fields

	public static final String TRIP_VENDOR = "vendor";

	// trip Extra

	public static final String TRIP_HOUR_START = "hour_start";
	public static final String TRIP_DISTANCE = "distance (km)";
	public static final String TRIP_DURATION = "duration (min)";
	public static final String TRIP_SPEED = "speed (km/h)";
}
