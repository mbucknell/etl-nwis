package gov.acwi.wqp.etl.projectData;


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
public class TransformProjectData {

    @Autowired
    @Qualifier(EtlConstantUtils.SETUP_PROJECT_DATA_SWAP_TABLE_FLOW)
    private Flow setupProjectDataSwapTableFlow;

    @Autowired
    @Qualifier(EtlConstantUtils.BUILD_PROJECT_DATA_INDEXES_FLOW)
    private Flow buildProjectDataIndexesFlow;

    @Bean
    public Flow projectDataFlow() throws IOException {
        return new FlowBuilder<SimpleFlow>("projectDataFlow")
                .start(setupProjectDataSwapTableFlow)
                .next(buildProjectDataIndexesFlow)
                .build();
    }
}
