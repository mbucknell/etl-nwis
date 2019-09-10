package gov.acwi.wqp.etl.monitoringLocationObject;


import java.io.IOException;

import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformMonitoringLocationObject {

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_MONITORING_LOCATION_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupMonitoringLocationObjectSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_MONITORING_LOCATION_OBJECT_FLOW)
	private Flow afterLoadMonitoringLocationObjectFlow;

	@Bean
	public Flow monitoringLocationObjectFlow() throws IOException {
		return new FlowBuilder<SimpleFlow>("monitoringLocationObjectFlow")
				.start(setupMonitoringLocationObjectSwapTableFlow)
				.next(afterLoadMonitoringLocationObjectFlow)
				.build();
	}
}
