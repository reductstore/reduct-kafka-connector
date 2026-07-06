package store.reduct.connector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReductSinkConnectorTest {

	ReductSinkConnector connector;

	@BeforeEach
	void setUp() {
		connector = new ReductSinkConnector();
	}

	@Test
	void version() {
		assertThat(connector.version()).isEqualTo("0.1.0");
	}

	@Test
	void taskClass() {
		assertThat(connector.taskClass()).isEqualTo(ReductSinkTask.class);
	}

	@Test
	void taskConfigs() {
		Map<String, String> props = Map.of("reduct.store.url", "http://localhost:8383", "reduct.store.bucket",
				"test-bucket", "reduct.kafka.topic", "output");
		connector.start(props);
		List<Map<String, String>> configs = connector.taskConfigs(3);
		assertThat(configs).hasSize(3);
		assertThat(configs.get(0)).isEqualTo(props);
	}

	@Test
	void config() {
		ConfigDef configDef = connector.config();
		assertThat(configDef.configKeys()).containsKey("reduct.store.url");
		assertThat(configDef.configKeys()).containsKey("reduct.store.bucket");
		assertThat(configDef.configKeys()).containsKey("reduct.store.api-token");
		assertThat(configDef.configKeys()).containsKey("reduct.entry.name");
		assertThat(configDef.configKeys()).containsKey("reduct.record.content-type");
	}
}
