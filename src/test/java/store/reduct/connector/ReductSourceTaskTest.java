package store.reduct.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import store.reduct.connector.config.ReductSourceConfig;
import store.reduct.model.bucket.Bucket;
import store.reduct.model.record.Record;

@ExtendWith(MockitoExtension.class)
class ReductSourceTaskTest {

	@Mock
	Bucket bucket;
	@Mock
	SourceTaskContext context;
	@Mock
	OffsetStorageReader offsetReader;

	ReductSourceTask task;

	@BeforeEach
	void setUp() {
		task = new ReductSourceTask();
		task.config = new ReductSourceConfig(Map.of("reduct.store.url", "http://localhost:8383", "reduct.store.bucket",
				"test-bucket", "reduct.entry.name", "sensor-temp", "reduct.kafka.topic", "output-topic",
				"reduct.poll.interval.ms", "0"));
		task.bucket = bucket;
		task.setTaskContext(context);
		task.entryNames = List.of("sensor-temp");
	}

	@Test
	void version() {
		assertThat(task.version()).isEqualTo("0.1.0");
	}

	@Test
	void poll_returnsRecordsFromSingleEntry() throws InterruptedException {
		when(context.offsetStorageReader()).thenReturn(offsetReader);
		when(offsetReader.offset(any())).thenReturn(null);

		Record record = Record.builder().timestamp(1000000L).body("test-data".getBytes()).type("text/plain").length(9)
				.build();
		when(bucket.query(any(), any(), any(), any())).thenReturn(List.of(record).iterator());

		List<SourceRecord> records = task.poll();

		assertThat(records).isNotNull();
		assertThat(records).hasSize(1);
		assertThat(records.get(0).topic()).isEqualTo("output-topic");
		assertThat(records.get(0).value()).isEqualTo("test-data".getBytes());
	}

	@Test
	void poll_returnsNullWhenNoRecords() throws InterruptedException {
		when(context.offsetStorageReader()).thenReturn(offsetReader);
		when(offsetReader.offset(any())).thenReturn(null);
		when(bucket.query(any(), any(), any(), any())).thenReturn(Collections.emptyIterator());

		List<SourceRecord> records = task.poll();

		assertThat(records).isNull();
	}

	@Test
	void poll_resumesFromLastOffset() throws InterruptedException {
		when(context.offsetStorageReader()).thenReturn(offsetReader);
		when(offsetReader.offset(Map.of("bucket", "test-bucket", "entry", "sensor-temp")))
				.thenReturn(Map.of("timestamp", 5000000L));

		Record record = Record.builder().timestamp(6000000L).body("data".getBytes()).type("text/plain").length(4)
				.build();
		when(bucket.query(any(), any(), any(), any())).thenReturn(List.of(record).iterator());

		List<SourceRecord> records = task.poll();

		assertThat(records).isNotNull();
		assertThat(records).hasSize(1);
		Map<String, ?> offset = records.get(0).sourceOffset();
		assertThat(offset).containsKey("timestamp");
		assertThat((Long) offset.get("timestamp")).isEqualTo(6000000L);
	}
}
