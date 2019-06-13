package gov.acwi.wqp.etl.biologicalHabitatMetric;

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
public class TransformBiologicalHabitatMetric {

    @Autowired
    @Qualifier(EtlConstantUtils.SETUP_BIOLOGICAL_HABITAT_METRIC_SWAP_TABLE_FLOW)
    private Flow setupBiologicalHabitatMetricSwapTableFlow;

    @Autowired
    @Qualifier(EtlConstantUtils.AFTER_LOAD_BIOLOGICAL_HABITAT_METRIC_FLOW)
    private Flow afterLoadBiologicalHabitatMetricFlow;

    @Bean
    public Flow biologicalHabitatMetricFlow() throws IOException {
        return new FlowBuilder<SimpleFlow>("biologicalHabitatMetricFlow")
                .start(setupBiologicalHabitatMetricSwapTableFlow)
                .next(afterLoadBiologicalHabitatMetricFlow)
                .build();
    }

}

