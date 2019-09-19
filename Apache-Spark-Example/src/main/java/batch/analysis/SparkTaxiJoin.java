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
			sparkConfigs = new HashMap<String, String>();
			sparkConfigs.put("spark.sql.autoBroadcastJoinThreshold", "-1");
		}

		SparkSession spark = SparkBS.getOrCreateInstance(settings.isLocal(), settings.getAppName(), sparkConfigs).getSession();

		final String dataFolder = settings.getDataFolder();

		Dataset<Row> tripData = spark.read().parquet(dataFolder + Constants.TRIP_DATA_PQ);

		Dataset<Row> tripVendors = spark.read().parquet(dataFolder + Constants.TRIP_VENDORS_PQ);

		Dataset<Row> tripJoin = tripData.join(tripVendors.limit(100), TaxiSchema.TRIP_ID);
		tripJoin.explain();
		tripJoin.show();

		// περιμένουμε να γίνει broadcast join στο δεύτερο πίνακα διότι είναι μικρός
		// (το default μέγιστο μέγεθος για το οποίο έχουμε broadcast ενός πίνακα είναι 10 ΜΒ)
		// + περιγραφή broadcast join

		// με την αλλαγή χρησιμοποιεί πλέον SortMergeJoin βλέπε wiki πως γίνεται αυτό.
		// κατανεμημένα; και πάλι γίνεται shuffle αλλά αντί για hash έχουμε sort. Να αναφέρω και για τι προτιμάται από το hash.
		// αν και απαιτεί και αυτό shuffle εμφανίζει πλεονεκτήματα καθώς μπορεί να γίνει split στο δίσκο αν υπερβαίνει το μέγεθος της μνήμης
		// σε αντίθεση με το hash table που πρέπει υποχρεωτικά να χωράει.

		// αλλάζοντας τις ρυθμίσεις (ώστε να κάνει repartion join pleon?) περιμένουμε μεγαλύτερους χρόνους εκτέλεσης
		// Στο repartion join οι εγγραφές χωρίζονται σε partitions με βάση το join key και κάθε τέτοιο partition στέλνεται σε κάποιον worker
		// έτσι έχουμε shuffle πολλών εγγραφών και από τους δύο πίνακες μεταξύ κόμβων του cluster.
		// + περιγραφή repartition join.

	}

}
