package store.reduct.connector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import store.reduct.connector.config.ReductSinkConfig;
import store.reduct.connector.version.Version;

public class ReductSinkConnector extends SinkConnector {

	private Map<String, String> configProps;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		configProps = props;
		new ReductSinkConfig(props);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ReductSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		return Collections.nCopies(maxTasks, configProps);
	}

	@Override
	public void stop() {
	}

	@Override
	public ConfigDef config() {
		return ReductSinkConfig.configDef();
	}
}
