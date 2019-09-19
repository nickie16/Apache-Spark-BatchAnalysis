package batch.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Functions {

	private static final Logger LOG = LoggerFactory.getLogger(Functions.class.getName());

	@Deprecated
	public static final Long timestampDiff(String start_time, String end_time) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_FORMAT); // SimpleDateFormat is not thread safe so each worker uses it's own
		try {
			Date parsedDate_s = dateFormat.parse(start_time);
			Timestamp time_start = new Timestamp(parsedDate_s.getTime());
			Date parsedDate_e = dateFormat.parse(end_time);
			Timestamp time_end = new Timestamp(parsedDate_e.getTime());
			long duration = (time_end.getTime() - time_start.getTime()) / 1000; // cast seconds
			return duration;
		} catch (ParseException e) {
			return null;
		}
	}

	public static final Long timeDiff(String startTime, String endTime) {
		try {
			LocalDateTime timeStart = LocalDateTime.parse(startTime, Constants.formatter);
			LocalDateTime timeEnd = LocalDateTime.parse(endTime, Constants.formatter);
			Duration d = Duration.between(timeStart, timeEnd);
			return d.getSeconds();

		} catch (DateTimeParseException e) {
			LOG.warn("Parse exception was caught. {}", e.getMessage());
			throw e;
			//return null;
		}
	}

	public static final double haversine(Double lonStart, Double latStart, Double lonEnd, Double latEnd) {

		double dLon = Math.toRadians(lonEnd - lonStart); // λ
		double dLat = Math.toRadians(latEnd - latStart); // φ

		latStart = Math.toRadians(latStart);
		latEnd = Math.toRadians(latEnd);

		double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(latStart) * Math.cos(latEnd) * Math.pow(Math.sin(dLon / 2), 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		double result = Constants.EARTH_R * c;
		return result;
	}

	public static void main(String[] args) {
		LOG.debug("{}", timeDiff("2015-03-08 23:54:12", "2015-03-09 00:12:35"));
		LOG.debug("{}", haversine(-69.6518630981445, 42.982208251953125, -73.95278930664062, 40.80302047729492));
	}

}
