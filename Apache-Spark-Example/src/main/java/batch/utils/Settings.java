package batch.utils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class Settings {

	@Parameter(names = "-local", required = false, description = "Enable local execution")
	protected boolean local = false;

	@Parameter(names = "-appName", required = false, description = "The spark application name")
	protected String appName = "";

	@Parameter(names = "-dataFolder", required = false, description = "IO folder")
	protected String dataFolder = "src/test/resources/";

	@Parameter(names = "-tripData", required = false, description = "")
	private String tripData = "yellow_tripdata_1m.csv";

	@Parameter(names = "-tripVendor", required = false, description = "")
	private String tripVendor = "yellow_tripvendors_1m.csv";

	@Parameter(names = "-disableAutoBroadcastJoin", required = false, description = "")
	private boolean disableAutoBroadcastJoin = false;

	@Parameter(names = "-createUseParquet", required = false, description = "")
	protected boolean createUseParquet = false;

	public boolean isLocal() {
		return local;
	}

	public String getAppName() {
		return appName;
	}

	public String getDataFolder() {
		return dataFolder;
	}

	public String getTripData() {
		return tripData;
	}

	public String getTripVendor() {
		return tripVendor;
	}

	public boolean isDisableAutoBroadcastJoin() {
		return disableAutoBroadcastJoin;
	}

	public boolean isCreateUseParquet() {
		return createUseParquet;
	}
}
