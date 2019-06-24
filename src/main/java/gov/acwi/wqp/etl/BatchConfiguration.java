package gov.acwi.wqp.etl;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	@Qualifier("mySqlNwisExtractFlow")
	private Flow mySqlNwisExtractFlow;

	@Autowired
	@Qualifier("sampleParameterFlow")
	private Flow sampleParameterFlow;

	@Autowired
	@Qualifier("nwisStationLocalFlow")
	private Flow nwisStationLocalFlow;
	
	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Autowired
	@Qualifier("projectDataFlow")
	private Flow projectDataFlow;
	
	@Autowired
	@Qualifier("monitoringLocationFlow")
	private Flow monitoringLocationFlow;

	@Autowired
	@Qualifier("biologicalHabitatMetricFlow")
	private Flow biologicalHabitatMetricFlow;
	
	@Autowired
	@Qualifier("activityFlow")
	private Flow activityFlow;

	@Autowired
	@Qualifier("activityMetricFlow")
	private Flow activityMetricFlow;
	
	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;
	
	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;

	@Autowired
	@Qualifier("projectMLWeightingFlow")
	private Flow projectMLWeightingFlow;
	
	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_SUMMARIES_FLOW)
	private Flow createSummariesFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_LOOKUP_CODES_FLOW)
	private Flow createLookupCodesFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.CREATE_DATABASE_FINALIZE_FLOW)
	private Flow databaseFinalizeFlow;

	@Autowired
	@Qualifier("publicSrsnamesFlow")
	private Flow publicSrsnamesFlow;

	@Bean
	public Flow nwisToWqpFlow() {
		return new FlowBuilder<SimpleFlow>("nwisToWqpFlow")
				.start(sampleParameterFlow)
				.next(nwisStationLocalFlow)
				.next(orgDataFlow)
				.next(projectDataFlow)
				.next(monitoringLocationFlow)
				.next(biologicalHabitatMetricFlow)
				.next(activityFlow)
				.next(activityMetricFlow)
				.next(resultFlow)
				.next(resDetectQntLimitFlow)
				.next(projectMLWeightingFlow)
				.next(createSummariesFlow)
				.next(createLookupCodesFlow)
				.next(databaseFinalizeFlow)
				.next(publicSrsnamesFlow)
				.build();

	}

	@Bean
	public Job nwisEtl() {
		return jobBuilderFactory.get("WQP_NWIS_ETL")
				.start(mySqlNwisExtractFlow)
				.next(nwisToWqpFlow())
				.build()
				.build();
	}
		
}
