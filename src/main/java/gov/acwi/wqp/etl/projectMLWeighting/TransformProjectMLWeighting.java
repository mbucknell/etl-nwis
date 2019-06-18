package gov.acwi.wqp.etl.projectMLWeighting;

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
public class TransformProjectMLWeighting {

    @Autowired
    @Qualifier(EtlConstantUtils.SETUP_PROJECT_ML_WEIGHTING_SWAP_TABLE_FLOW)
    private Flow setupProjectMLWeightingSwapTableFlow;

    @Autowired
    @Qualifier(EtlConstantUtils.AFTER_LOAD_PROJECT_ML_WEIGHTING_FLOW)
    private Flow afterLoadProjectMLWeightingFlow;

    @Bean
    public Flow projectMLWeightingFlow() throws IOException {
        return new FlowBuilder<SimpleFlow>("projectMLWeightingFlow")
                .start(setupProjectMLWeightingSwapTableFlow)
                .next(afterLoadProjectMLWeightingFlow)
                .build();
    }
}
