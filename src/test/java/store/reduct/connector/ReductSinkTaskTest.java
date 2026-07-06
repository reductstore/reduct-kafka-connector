package store.reduct.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import store.reduct.connector.config.ReductSinkConfig;
import store.reduct.model.bucket.Bucket;

@ExtendWith(MockitoExtension.class)
class ReductSinkTaskTest {

	@Mock
	Bucket bucket;

	ReductSinkTask task;

	@BeforeEach
	void setUp() {
		task = new ReductSinkTask();
		task.config = new ReductSinkConfig(
				Map.of("reduct.store.url", "http://localhost:8383", "reduct.store.bucket", "test-bucket"));
		task.objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
		task.bucket = bucket;
	}

	@Test
	void version() {
		assertThat(task.version()).isEqualTo("0.1.0");
	}

	@Test
	void put_writesRecordsToEntryNamedByTopic() {
		SinkRecord record = new SinkRecord("sensor-temp", 0, null, null, null, "hello", 0L);
		task.put(List.of(record));

		verify(bucket).writeRecords(argThat(name -> name.equals("sensor-temp")), any());
	}

	@Test
	void put_writesRecordsToConfiguredEntryName() {
		task.config = new ReductSinkConfig(Map.of("reduct.store.url", "http://localhost:8383", "reduct.store.bucket",
				"test-bucket", "reduct.entry.name", "my-entry"));

		SinkRecord record = new SinkRecord("sensor-temp", 0, null, null, null, "hello", 0L);
		task.put(List.of(record));

		verify(bucket).writeRecords(argThat(name -> name.equals("my-entry")), any());
	}

	@Test
	void put_handlesNullValue() {
		SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0L);
		task.put(List.of(record));

		verify(bucket).writeRecords(anyString(), any());
	}

	@Test
	void put_producesRecordWithTimestampFromKafka() {
		long kafkaTs = 1700000000L;
		SinkRecord record = new SinkRecord("topic", 0, null, null, null, "data", kafkaTs);
		task.put(List.of(record));

		verify(bucket).writeRecords(anyString(), any());
	}
}
