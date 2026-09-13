package store.reduct.connector;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import store.reduct.client.ReductClient;
import store.reduct.client.config.ServerProperties;
import store.reduct.connector.config.ReductSourceConfig;
import store.reduct.connector.version.Version;
import store.reduct.model.bucket.Bucket;
import store.reduct.model.bucket.EntryInfo;
import store.reduct.model.record.Record;

@Slf4j
public class ReductSourceTask extends SourceTask {

	static final int MAX_BATCH_SIZE = 1000;

	ReductClient client;
	Bucket bucket;
	ReductSourceConfig config;
	List<String> entryNames;
	final Map<String, Long> lastReadOffsets = new HashMap<>();

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		config = new ReductSourceConfig(props);

		ServerProperties serverProperties = ServerProperties.builder().url(config.getServerUrl())
				.apiToken(config.getApiToken()).build();
		client = new ReductClient(serverProperties, HttpClient.newHttpClient());
		bucket = client.getBucket(config.getBucket());
		log.info("Connected to bucket '{}'", config.getBucket());

		if (config.isAllEntries()) {
			entryNames = new ArrayList<>();
			log.info("Source configured to read from all entries in bucket '{}'", config.getBucket());
		} else {
			entryNames = Collections.singletonList(config.getEntryName());
			log.info("Source configured to read from single entry '{}'", config.getEntryName());
		}
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		if (config.isAllEntries()) {
			refreshEntryNames();
		}

		List<SourceRecord> records = new ArrayList<>();

		for (String entryName : entryNames) {
			if (entryName == null) {
				continue;
			}

			Map<String, String> sourcePartition = Map.of("bucket", config.getBucket(), "entry", entryName);

			long startTs;
			Long lastRead = lastReadOffsets.get(entryName);
			if (lastRead != null) {
				startTs = lastRead + 1;
			} else {
				Map<String, Object> lastOffset = context.offsetStorageReader().offset(sourcePartition);
				startTs = lastOffset != null ? (Long) lastOffset.get("timestamp") + 1 : config.getStartTimestamp();
			}

			long stopTs = System.currentTimeMillis() * 1000;
			Iterator<Record> iterator = bucket.query(entryName, startTs, stopTs, (long) config.getTtl());
			while (iterator.hasNext() && records.size() < MAX_BATCH_SIZE) {
				Record rec = iterator.next();
				org.apache.kafka.connect.data.Schema schema = null;
				SourceRecord sourceRecord = new SourceRecord(sourcePartition, Map.of("timestamp", rec.getTimestamp()),
						config.getKafkaTopic(), null, schema, null, schema, rec.getBody(), rec.getTimestamp() / 1000);
				records.add(sourceRecord);
				lastReadOffsets.put(entryName, rec.getTimestamp());
			}
		}

		if (records.isEmpty()) {
			Thread.sleep(config.getPollInterval());
		}

		return records.isEmpty() ? null : records;
	}

	private void refreshEntryNames() {
		Bucket freshBucket = bucket.read();
		entryNames = freshBucket.getEntryInfos().stream().map(EntryInfo::getName).collect(Collectors.toList());
	}

	void setTaskContext(org.apache.kafka.connect.source.SourceTaskContext ctx) {
		this.context = ctx;
	}

	@Override
	public void stop() {
	}
}
