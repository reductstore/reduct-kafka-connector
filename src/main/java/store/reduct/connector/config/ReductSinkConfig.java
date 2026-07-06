package store.reduct.connector.config;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ReductSinkConfig extends AbstractConfig {

	public static final String SERVER_URL = "reduct.store.url";
	public static final String API_TOKEN = "reduct.store.api-token";
	public static final String BUCKET = "reduct.store.bucket";
	public static final String ENTRY_NAME = "reduct.entry.name";
	public static final String CONTENT_TYPE = "reduct.record.content-type";

	private static final String SERVER_URL_DOC = "ReductStore server URL (e.g. http://localhost:8383)";
	private static final String API_TOKEN_DOC = "ReductStore API token for authentication";
	private static final String BUCKET_DOC = "Target bucket name in ReductStore";
	private static final String ENTRY_NAME_DOC = "Entry name to write records to. If not set, the Kafka topic name is used as the entry name.";
	private static final String CONTENT_TYPE_DOC = "Content type for written records (default: application/octet-stream)";

	public static ConfigDef configDef() {
		return new ConfigDef()
				.define(SERVER_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
						SERVER_URL_DOC)
				.define(API_TOKEN, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, API_TOKEN_DOC)
				.define(BUCKET, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
						BUCKET_DOC)
				.define(ENTRY_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, ENTRY_NAME_DOC)
				.define(CONTENT_TYPE, ConfigDef.Type.STRING, "application/octet-stream", ConfigDef.Importance.LOW,
						CONTENT_TYPE_DOC);
	}

	public ReductSinkConfig(Map<String, String> props) {
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

	public String getContentType() {
		return getString(CONTENT_TYPE);
	}
}
