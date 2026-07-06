package store.reduct.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import store.reduct.client.ReductClient;
import store.reduct.client.config.ServerProperties;
import store.reduct.common.exception.ReductException;
import store.reduct.connector.config.ReductSinkConfig;
import store.reduct.connector.version.Version;
import store.reduct.model.bucket.Bucket;
import store.reduct.model.bucket.BucketSettings;
import store.reduct.model.record.Record;

@Slf4j
public class ReductSinkTask extends SinkTask {

	ReductClient client;
	Bucket bucket;
	ReductSinkConfig config;
	ObjectMapper objectMapper;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		config = new ReductSinkConfig(props);
		objectMapper = new ObjectMapper();

		ServerProperties serverProperties = ServerProperties.builder().url(config.getServerUrl())
				.apiToken(config.getApiToken()).build();
		client = new ReductClient(serverProperties, HttpClient.newHttpClient());

		try {
			bucket = client.getBucket(config.getBucket());
			log.info("Connected to existing bucket '{}'", config.getBucket());
		} catch (ReductException e) {
			if (e.getStatusCode() == 404) {
				bucket = client.createBucket(config.getBucket(), BucketSettings.builder().build());
				log.info("Created bucket '{}'", config.getBucket());
			} else {
				log.error("Failed to get or create bucket '{}': {}", config.getBucket(), e.getMessage());
				throw e;
			}
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		Map<String, List<SinkRecord>> grouped = records.stream().collect(Collectors.groupingBy(this::resolveEntryName));

		for (var entry : grouped.entrySet()) {
			String entryName = entry.getKey();
			List<Record> reductRecords = entry.getValue().stream().map(this::toReductRecord)
					.collect(Collectors.toList());

			log.debug("Writing {} records to entry '{}'", reductRecords.size(), entryName);
			bucket.writeRecords(entryName, reductRecords.iterator());
		}
	}

	private String resolveEntryName(SinkRecord record) {
		return config.getEntryName() != null ? config.getEntryName() : record.topic();
	}

	private Record toReductRecord(SinkRecord record) {
		long ts = record.timestamp() != null ? record.timestamp() * 1000 : System.currentTimeMillis() * 1000;
		byte[] body = toBytes(record.value());
		return Record.builder().timestamp(ts).body(body).type(config.getContentType()).length(body.length).build();
	}

	private byte[] toBytes(Object value) {
		if (value == null) {
			return new byte[0];
		}
		if (value instanceof byte[]) {
			return (byte[]) value;
		}
		if (value instanceof String) {
			return ((String) value).getBytes(StandardCharsets.UTF_8);
		}
		try {
			return objectMapper.writeValueAsBytes(value);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Failed to serialize record value", e);
		}
	}

	@Override
	public void stop() {
	}
}
