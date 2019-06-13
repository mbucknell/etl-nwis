package gov.acwi.wqp.etl.activityMetric;

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
public class TransformActivityMetric {

    @Autowired
    @Qualifier(EtlConstantUtils.SETUP_ACTIVITY_METRIC_SWAP_TABLE_FLOW)
    private Flow setupActivityMetricSwapTableFlow;

    @Autowired
    @Qualifier(EtlConstantUtils.AFTER_LOAD_ACTIVITY_METRIC_FLOW)
    private Flow afterLoadActivityMetricFlow;

    @Bean
    public Flow activityMetricFlow() throws IOException {
        return new FlowBuilder<SimpleFlow>("activityMetricFlow")
                .start(setupActivityMetricSwapTableFlow)
                .next(afterLoadActivityMetricFlow)
                .build();
    }

}
