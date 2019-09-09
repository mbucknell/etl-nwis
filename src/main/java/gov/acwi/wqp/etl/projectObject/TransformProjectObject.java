package gov.acwi.wqp.etl.projectObject;


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
public class TransformProjectObject {

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_PROJECT_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupProjectObjectSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_PROJECT_OBJECT_FLOW)
	private Flow afterLoadProjectObjectFlow;

	@Bean
	public Flow projectObjectFlow() throws IOException {
		return new FlowBuilder<SimpleFlow>("projectObjectFlow")
				.start(setupProjectObjectSwapTableFlow)
				.next(afterLoadProjectObjectFlow)
				.build();
	}
}
