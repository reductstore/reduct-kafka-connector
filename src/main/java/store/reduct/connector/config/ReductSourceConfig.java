package store.reduct.connector.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ReductSourceConfig extends AbstractConfig {

	public static final String SERVER_URL = "reduct.store.url";
	public static final String API_TOKEN = "reduct.store.api-token";
	public static final String BUCKET = "reduct.store.bucket";
	public static final String ENTRY_NAME = "reduct.entry.name";
	public static final String ALL_ENTRIES = "reduct.entries.all";
	public static final String POLL_INTERVAL = "reduct.poll.interval.ms";
	public static final String START_TIMESTAMP = "reduct.query.start.timestamp";
	public static final String TTL = "reduct.query.ttl";
	public static final String KAFKA_TOPIC = "reduct.kafka.topic";

	private static final String SERVER_URL_DOC = "ReductStore server URL (e.g. http://localhost:8383)";
	private static final String API_TOKEN_DOC = "ReductStore API token for authentication";
	private static final String BUCKET_DOC = "Bucket name to read from in ReductStore";
	private static final String ENTRY_NAME_DOC = "Entry name to read records from. Ignored if reduct.entries.all is true.";
	private static final String ALL_ENTRIES_DOC = "If true, read from all entries in the bucket. Default: false";
	private static final String POLL_INTERVAL_DOC = "Poll interval in milliseconds (default: 1000)";
	private static final String START_TIMESTAMP_DOC = "Start timestamp in microseconds for the initial query when no offset is stored (default: 0)";
	private static final String TTL_DOC = "Query TTL in seconds (default: 5)";
	private static final String KAFKA_TOPIC_DOC = "Target Kafka topic name for produced records";

	public static ConfigDef configDef() {
		return new ConfigDef()
				.define(SERVER_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
						SERVER_URL_DOC)
				.define(API_TOKEN, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, API_TOKEN_DOC)
				.define(BUCKET, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
						BUCKET_DOC)
				.define(ENTRY_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, ENTRY_NAME_DOC)
				.define(ALL_ENTRIES, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, ALL_ENTRIES_DOC)
				.define(POLL_INTERVAL, ConfigDef.Type.LONG, 1000L, ConfigDef.Importance.LOW, POLL_INTERVAL_DOC)
				.define(START_TIMESTAMP, ConfigDef.Type.LONG, 0L, ConfigDef.Importance.MEDIUM, START_TIMESTAMP_DOC)
				.define(TTL, ConfigDef.Type.INT, 5, ConfigDef.Importance.LOW, TTL_DOC).define(KAFKA_TOPIC,
						ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC);
	}

	public ReductSourceConfig(Map<String, String> props) {
		super(configDef(), props);
	}

	public String getServerUrl() {
		return getString(SERVER_URL);
	}

	public String getApiToken() {
		return getPassword(API_TOKEN).value();
	}

	public String getBucket() {
		return getString(BUCKET);
	}

	public String getEntryName() {
		return getString(ENTRY_NAME);
	}

	public boolean isAllEntries() {
		return getBoolean(ALL_ENTRIES);
	}

	public long getPollInterval() {
		return getLong(POLL_INTERVAL);
	}

	public long getStartTimestamp() {
		return getLong(START_TIMESTAMP);
	}

	public int getTtl() {
		return getInt(TTL);
	}

	public String getKafkaTopic() {
		return getString(KAFKA_TOPIC);
	}
}
