package store.reduct.connector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import store.reduct.connector.config.ReductSourceConfig;
import store.reduct.connector.version.Version;

public class ReductSourceConnector extends SourceConnector {

	private Map<String, String> configProps;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		configProps = props;
		new ReductSourceConfig(props);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ReductSourceTask.class;
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
		return ReductSourceConfig.configDef();
	}
}
