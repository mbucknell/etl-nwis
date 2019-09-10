package gov.acwi.wqp.etl.resultObject;


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
public class TransformResultObject {

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RESULT_OBJECT_SWAP_TABLE_FLOW)
	private Flow setupResultObjectSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_RESULT_OBJECT_FLOW)
	private Flow afterLoadResultObjectFlow;

	@Bean
	public Flow resultObjectFlow() throws IOException {
		return new FlowBuilder<SimpleFlow>("resultObjectFlow")
				.start(setupResultObjectSwapTableFlow)
				.next(afterLoadResultObjectFlow)
				.build();
	}
}
